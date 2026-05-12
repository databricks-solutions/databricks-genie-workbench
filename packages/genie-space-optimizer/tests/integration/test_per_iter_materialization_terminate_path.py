"""Plan P-A integration smoke — drive the harness's per-iter writer
with synthesized in-memory state for three iterations (one accepted,
one rolled back, one skipped) and assert all 8 contract paths
materialize for every iteration regardless of exit_path.

The writer is exposed as a module-level helper
``_materialize_per_iter_contract_paths`` so the test can stub the
MLflow client without driving the entire 26000-line harness body.
"""
from __future__ import annotations


def _make_iter_summary(*, iteration: int, exit_path: str) -> dict:
    return {
        "iteration": iteration,
        "accepted_count": 1 if exit_path == "completed" else 0,
        "rolled_back_count": 1 if exit_path == "rolled_back" else 0,
        "skipped_count": 1 if exit_path == "skipped_no_applied_patches" else 0,
        "gate_drop_count": 0,
        "decision_record_count": 0,
        "journey_violation_count": 0,
        "exit_path": exit_path,
    }


def test_materialize_writes_eight_paths_per_iteration_for_mixed_exit_paths() -> None:
    from genie_space_optimizer.optimization import harness as h
    from genie_space_optimizer.optimization.run_output_contract import (
        bundle_artifact_paths,
    )

    captured: list[tuple[str, str]] = []

    class _StubClient:
        def log_text(
            self, *, run_id: str, text: str, artifact_file: str,
        ) -> None:
            captured.append((artifact_file, text[:120]))

    iterations = [1, 2, 3]
    iter_summaries = {
        1: _make_iter_summary(iteration=1, exit_path="completed"),
        2: _make_iter_summary(iteration=2, exit_path="rolled_back"),
        3: _make_iter_summary(iteration=3, exit_path="skipped_no_applied_patches"),
    }
    iter_decision_records = {1: [], 2: [], 3: []}
    iter_journey_reports = {1: {"violations": []}, 2: {"violations": []}, 3: None}
    iter_rca_ledgers = {1: {"themes": []}, 2: {}, 3: None}
    iter_proposal_inventories = {1: {"proposals": []}, 2: None, 3: None}
    iter_transcripts = {
        1: "# iter 1\n", 2: "# iter 2\n", 3: "# iter 3\n",
    }
    iter_stage_index = {
        (1, "evaluation_state"): "ok", (1, "rca_evidence"): "ok",
        (2, "evaluation_state"): "ok",
        (3, "evaluation_state"): "ok",
    }
    iter_invariant_violations = {1: (), 2: (), 3: ({"kind": "ungrounded_rca"},)}

    h._materialize_per_iter_contract_paths(
        client=_StubClient(),
        anchor_run_id="anchor_run_xyz",
        iterations=iterations,
        iter_summaries=iter_summaries,
        iter_decision_records=iter_decision_records,
        iter_journey_reports=iter_journey_reports,
        iter_rca_ledgers=iter_rca_ledgers,
        iter_proposal_inventories=iter_proposal_inventories,
        iter_transcripts=iter_transcripts,
        stage_capture_index=iter_stage_index,
        iter_invariant_violations=iter_invariant_violations,
    )

    declared = bundle_artifact_paths(iterations=iterations)
    written_paths = {p for p, _ in captured}
    for it in iterations:
        per_iter = declared["iterations"][it]
        for kind in (
            "summary", "decision_trace", "journey_validation",
            "rca_ledger", "proposal_inventory", "patch_survival",
            "operator_transcript", "stages",
        ):
            assert per_iter[kind] in written_paths, (
                f"iter={it} kind={kind} missing from writer output"
            )


def test_materialize_is_idempotent_under_missing_in_memory_state() -> None:
    """Skipped iterations may have no rca_ledger / proposal_inventory
    / journey_report / decision_records — every per-iter file must
    still materialize using empty payloads. No exception, no missing
    file."""
    from genie_space_optimizer.optimization import harness as h
    from genie_space_optimizer.optimization.run_output_contract import (
        bundle_artifact_paths,
    )

    captured: list[tuple[str, str]] = []

    class _StubClient:
        def log_text(
            self, *, run_id: str, text: str, artifact_file: str,
        ) -> None:
            captured.append((artifact_file, text[:80]))

    iterations = [1]
    h._materialize_per_iter_contract_paths(
        client=_StubClient(),
        anchor_run_id="anchor_xyz",
        iterations=iterations,
        iter_summaries={1: {"iteration": 1, "exit_path": "in_progress"}},
        iter_decision_records={},  # empty dict — iter 1 missing
        iter_journey_reports={},
        iter_rca_ledgers={},
        iter_proposal_inventories={},
        iter_transcripts={},
        stage_capture_index={},
        iter_invariant_violations={},
    )

    declared = bundle_artifact_paths(iterations=iterations)
    written_paths = {p for p, _ in captured}
    iter_1_paths = declared["iterations"][1]
    for kind in (
        "summary", "decision_trace", "journey_validation",
        "rca_ledger", "proposal_inventory", "patch_survival",
        "operator_transcript", "stages",
    ):
        assert iter_1_paths[kind] in written_paths


def test_materialize_swallows_log_text_exceptions_per_path() -> None:
    """A single per-path log_text failure must NOT abort the writer
    for the remaining paths or remaining iterations — Phase H is
    best-effort by contract."""
    from genie_space_optimizer.optimization import harness as h
    from genie_space_optimizer.optimization.run_output_contract import (
        bundle_artifact_paths,
    )

    written_paths: set[str] = set()
    fail_path = (
        "gso_postmortem_bundle/iterations/iter_01/rca_ledger.json"
    )

    class _PartiallyBrokenClient:
        def log_text(
            self, *, run_id: str, text: str, artifact_file: str,
        ) -> None:
            if artifact_file == fail_path:
                raise RuntimeError("simulated MLflow log_text failure")
            written_paths.add(artifact_file)

    iterations = [1, 2]
    h._materialize_per_iter_contract_paths(
        client=_PartiallyBrokenClient(),
        anchor_run_id="anchor",
        iterations=iterations,
        iter_summaries={
            1: {"iteration": 1, "exit_path": "completed"},
            2: {"iteration": 2, "exit_path": "rolled_back"},
        },
        iter_decision_records={1: [], 2: []},
        iter_journey_reports={},
        iter_rca_ledgers={1: {"themes": []}, 2: {}},
        iter_proposal_inventories={},
        iter_transcripts={1: "", 2: ""},
        stage_capture_index={},
        iter_invariant_violations={},
    )
    declared = bundle_artifact_paths(iterations=iterations)
    # The deliberately-failing path is NOT in written_paths but every
    # other declared per-iter path IS.
    assert fail_path not in written_paths
    for it in iterations:
        per_iter = declared["iterations"][it]
        for kind in (
            "summary", "decision_trace", "journey_validation",
            "proposal_inventory", "patch_survival",
            "operator_transcript", "stages",
        ):
            assert per_iter[kind] in written_paths
        # iter_1 rca_ledger fails; iter_2 succeeds.
        if it == 1:
            assert per_iter["rca_ledger"] not in written_paths
        else:
            assert per_iter["rca_ledger"] in written_paths
