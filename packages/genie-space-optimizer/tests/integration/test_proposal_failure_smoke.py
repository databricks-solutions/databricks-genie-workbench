"""Plan P-F — integration smoke test.

Verifies that with GSO_PROPOSAL_FAILURE_DECIDED=1 the helper produces
the expected records and markers for the failure-mode shapes observed
in the evidence runs ccf1d60d-d686-467b-bafa-1640131b4393 and
31ecd96f-5d56-4b5a-af8e-38e9e5c549af.

Evidence anchor: runid_analysis/{ccf1d60d,31ecd96f}/postmortem.md
"""

from __future__ import annotations


def test_smoke_ccf1d60d_proposal_generation_empty_emits_taxonomy(
    monkeypatch,
) -> None:
    """ccf1d60d iter 2: AG_COVERAGE_H001 hits proposal_generation_empty
    with an ungrounded RCA. Expect REQUEST_EVIDENCE_GATHERING."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_proposal_failure_decided,
    )

    iter_inputs: dict = {}
    _emit_proposal_failure_decided(
        run_id="ccf1d60d-d686-467b-bafa-1640131b4393",
        iteration=2,
        ag_id="AG_COVERAGE_H001",
        cluster_id="C_H001",
        cluster_signature="sig:h001",
        rca_id="",
        root_cause="",
        failure_mode="proposal_generation_empty",
        lever_set=(1, 3, 5, 6),
        tried_lever_families=(1,),
        ag_source_cluster_count=1,
        rca_card_grounded=False,
        prior_failure_count=0,
        target_qids=("gs_009", "gs_024"),
        iter_inputs=iter_inputs,
    )

    records = iter_inputs.get("decision_records") or []
    assert len(records) == 1
    assert records[0]["reason_code"] == "request_evidence_gathering"


def test_smoke_31ecd96f_skipped_no_applied_emits_taxonomy(monkeypatch) -> None:
    """31ecd96f iter 1: AG hits skipped_no_applied_patches on a single-
    cluster AG with multiple lever families remaining. Expect
    ROTATE_LEVER_FAMILY."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_proposal_failure_decided,
    )

    iter_inputs: dict = {}
    _emit_proposal_failure_decided(
        run_id="31ecd96f-5d56-4b5a-af8e-38e9e5c549af",
        iteration=1,
        ag_id="AG_SLA_BREACH",
        cluster_id="C_SLA",
        cluster_signature="sig:sla",
        rca_id="rca_sla",
        root_cause="missing_filter",
        failure_mode="no_applied_patches",
        lever_set=(1, 5, 6),
        tried_lever_families=(1,),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
        target_qids=("gs_026",),
        iter_inputs=iter_inputs,
    )

    records = iter_inputs.get("decision_records") or []
    markers = iter_inputs.get("markers") or []
    assert len(records) == 1
    assert records[0]["reason_code"] == "rotate_lever_family"
    assert any(m.startswith("GSO_PROPOSAL_FAILURE_DECIDED_V1 ") for m in markers)


def test_smoke_coverage_invariant_fires_on_silent_iter(monkeypatch) -> None:
    """When the helper is *not* called on a no-applied iteration, the
    coverage invariant emits the violation marker."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")

    from genie_space_optimizer.optimization.harness import (
        _check_and_emit_proposal_failure_coverage,
    )

    iter_inputs = {
        "applied_patches_total": 0,
        "exit_path": "no_causal_applyable_patch",
        "decision_records": [
            {"decision_type": "acceptance_decided"},
        ],
        "markers": [],
    }
    _check_and_emit_proposal_failure_coverage(
        run_id="ccf1d60d-d686-467b-bafa-1640131b4393",
        iteration=3,
        iter_inputs=iter_inputs,
    )

    assert any(
        "proposal_failure_decided_coverage" in m
        for m in iter_inputs["markers"]
    )


def test_smoke_coverage_invariant_central_finalize_wiring(
    monkeypatch, capsys,
) -> None:
    """PF-T12 — the centralized wiring inside _finalize_iteration_summary
    must fire the coverage invariant for a no-applied exit path that
    the legacy exit-path-specific call sites do NOT cover
    (dead_on_arrival), and must also print the
    GSO_INVARIANT_VIOLATION_V1 marker to stdout for the postmortem
    parser."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")

    from genie_space_optimizer.optimization.harness import (
        _finalize_iteration_summary,
    )

    iter_traces: dict = {}
    iter_summaries: dict = {}
    iter_inputs = {
        "applied_patches_total": 0,
        "decision_records": [
            {
                "run_id": "r1", "iteration": 7,
                "decision_type": "evaluation_classified",
                "outcome": "info", "reason_code": "tier_loss",
            },
        ],
    }

    _finalize_iteration_summary(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=7,
        current_iter_inputs=iter_inputs,
        journey_events=[],
        journey_report=None,
        accepted_count=0,
        rolled_back_count=0,
        skipped_count=0,
        gate_drop_count=0,
        iteration_accuracy_percent=None,
        exit_path="dead_on_arrival",
        run_id="ccf1d60d-d686-467b-bafa-1640131b4393",
    )

    markers = iter_inputs.get("markers") or []
    assert any(
        "proposal_failure_decided_coverage" in m for m in markers
    ), f"central wiring did not emit invariant marker; markers={markers!r}"

    captured = capsys.readouterr()
    assert "proposal_failure_decided_coverage" in captured.out, (
        "PF-T12: coverage invariant marker must be printed to stdout "
        "so the postmortem parser sees it"
    )


def test_smoke_coverage_invariant_central_finalize_noop_on_completed(
    monkeypatch, capsys,
) -> None:
    """PF-T12 — the centralized wiring is a hard no-op on a non-no-applied
    exit path like ``completed`` even when applied_patches_total=0,
    because the invariant's exit-path frozenset is the gate."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")

    from genie_space_optimizer.optimization.harness import (
        _finalize_iteration_summary,
    )

    iter_traces: dict = {}
    iter_summaries: dict = {}
    iter_inputs = {
        "applied_patches_total": 0,
        "decision_records": [
            {
                "run_id": "r1", "iteration": 1,
                "decision_type": "evaluation_classified",
                "outcome": "info", "reason_code": "tier_loss",
            },
        ],
    }

    _finalize_iteration_summary(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=1,
        current_iter_inputs=iter_inputs,
        journey_events=[],
        journey_report=None,
        accepted_count=0,
        rolled_back_count=0,
        skipped_count=0,
        gate_drop_count=0,
        iteration_accuracy_percent=None,
        exit_path="completed",
        run_id="r1",
    )

    markers = iter_inputs.get("markers") or []
    assert not any(
        "proposal_failure_decided_coverage" in m for m in markers
    ), f"central wiring fired on a non-no-applied exit path; markers={markers!r}"

    captured = capsys.readouterr()
    assert "proposal_failure_decided_coverage" not in captured.out
