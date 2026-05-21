"""Plan 12 Step 1.3.5 — cluster_failures must stamp _failing_qids and
_eval_rows_failing on metadata_snapshot from the eval_results when
the iteration scope didn't pre-stamp them. This closes the
"build_failing_qids_empty" decline path the dispatch helper
previously emitted when typed evidence was empty AND no metadata
was stamped — the typical production case.
"""
from unittest.mock import MagicMock, patch


def _eval_results_with_rows(rows):
    """Plain-dict eval_results shape (the most common path)."""
    return {"rows": rows}


def test_stamp_failing_qids_from_dict_eval_results(monkeypatch):
    """The plain-dict eval_results.rows path: cluster_failures should
    extract failing rows (score<0.5) and stamp them on
    metadata_snapshot before the Plan 11 dispatch runs."""
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    metadata_snapshot: dict = {"iteration": 1, "optimization_run_id": "run_x"}

    eval_results = _eval_results_with_rows([
        {"question_id": "gs_009", "question": "Top 10 customers", "score": 0.0,
         "ground_truth_sql": "GT", "generated_sql": "GEN",
         "judge_rationale": "wrong"},
        {"question_id": "gs_021", "question": "Revenue MTD", "score": 0.0,
         "ground_truth_sql": "GT", "generated_sql": "GEN",
         "judge_rationale": "wrong"},
        # passing — should NOT be stamped
        {"question_id": "gs_007", "question": "x", "score": 1.0},
        # soft signal — should NOT be stamped as failing (< 0.5)
        {"question_id": "gs_011", "question": "y", "score": 0.7},
    ])

    # Stub the LLM stages so the dispatch helper terminates without a
    # real LLM call.
    with patch.object(
        _stage1_mod, "diagnose_failing_qids", return_value=[],
    ), patch.object(
        _stage2_mod, "cluster_diagnoses", return_value=[],
    ):
        optimizer.cluster_failures(
            eval_results=eval_results,
            metadata_snapshot=metadata_snapshot,
            rca_evidence_typed=None,
            signal_type="hard",
            namespace="hard",
            w=MagicMock(),
        )

    assert metadata_snapshot["_failing_qids"] == ["gs_009", "gs_021"], (
        f"Stamped _failing_qids should only include score<0.5 rows; "
        f"got {metadata_snapshot.get('_failing_qids')!r}"
    )
    stamped_rows = metadata_snapshot["_eval_rows_failing"]
    assert len(stamped_rows) == 2
    assert {r["question_id"] for r in stamped_rows} == {"gs_009", "gs_021"}


def test_explicit_stamp_takes_precedence(monkeypatch):
    """If the iteration scope pre-stamps _failing_qids on
    metadata_snapshot, cluster_failures must NOT overwrite it. The
    derivation is a fallback only."""
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    metadata_snapshot: dict = {
        "iteration": 1,
        "_failing_qids": ["gs_explicit"],
        "_eval_rows_failing": [{"question_id": "gs_explicit"}],
    }

    with patch.object(
        _stage1_mod, "diagnose_failing_qids", return_value=[],
    ), patch.object(
        _stage2_mod, "cluster_diagnoses", return_value=[],
    ):
        optimizer.cluster_failures(
            eval_results=_eval_results_with_rows([
                {"question_id": "gs_009", "score": 0.0},
            ]),
            metadata_snapshot=metadata_snapshot,
            rca_evidence_typed=None,
            signal_type="hard",
            namespace="hard",
            w=MagicMock(),
        )

    assert metadata_snapshot["_failing_qids"] == ["gs_explicit"], (
        "Pre-stamped value must be preserved; cluster_failures must "
        "not overwrite it"
    )


def test_no_stamp_when_no_rows(monkeypatch):
    """Empty eval_results → both keys stay absent (or empty)."""
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer

    metadata_snapshot: dict = {"iteration": 1, "optimization_run_id": "run_x"}

    optimizer.cluster_failures(
        eval_results={"rows": []},
        metadata_snapshot=metadata_snapshot,
        rca_evidence_typed=None,
        signal_type="hard",
        namespace="hard",
        w=MagicMock(),
    )

    # Either absent or empty list — both signal "no failing qids".
    assert not metadata_snapshot.get("_failing_qids")
    assert not metadata_snapshot.get("_eval_rows_failing")


def test_stamp_handles_malformed_score(monkeypatch):
    """Non-numeric / missing score defaults to 0.0 (hard) so the count
    NEVER under-reports failures — same robustness contract as
    build_run_summary."""
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    metadata_snapshot: dict = {"iteration": 1, "optimization_run_id": "run_x"}

    eval_results = _eval_results_with_rows([
        {"question_id": "gs_001"},  # missing score
        {"question_id": "gs_002", "score": "not_a_number"},  # bad type
        {"question_id": "gs_003", "score": 1.0},  # passing
    ])

    with patch.object(
        _stage1_mod, "diagnose_failing_qids", return_value=[],
    ), patch.object(
        _stage2_mod, "cluster_diagnoses", return_value=[],
    ):
        optimizer.cluster_failures(
            eval_results=eval_results,
            metadata_snapshot=metadata_snapshot,
            rca_evidence_typed=None,
            signal_type="hard",
            namespace="hard",
            w=MagicMock(),
        )

    # gs_001 + gs_002 stamped (defensive: score < 0.5 means hard);
    # gs_003 (passing) excluded.
    assert metadata_snapshot["_failing_qids"] == ["gs_001", "gs_002"]


def test_dispatch_marker_now_says_entered_not_build_failing_qids_empty(
    capsys, monkeypatch,
):
    """The whole point of Step 1.3.5: in the typical production case
    (Plan 11 ON, rca_evidence_typed empty, but eval_results carries
    failing rows), the dispatch helper should emit ``outcome=entered``
    — NOT ``outcome=skipped skip_reason=build_failing_qids_empty``."""
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    with patch.object(
        _stage1_mod, "diagnose_failing_qids", return_value=[],
    ), patch.object(
        _stage2_mod, "cluster_diagnoses", return_value=[],
    ):
        optimizer.cluster_failures(
            eval_results=_eval_results_with_rows([
                {"question_id": "gs_009", "question": "q", "score": 0.0,
                 "ground_truth_sql": "GT", "generated_sql": "GEN",
                 "judge_rationale": "wrong"},
            ]),
            metadata_snapshot={"iteration": 1, "optimization_run_id": "run_x"},
            rca_evidence_typed=None,
            signal_type="hard",
            namespace="hard",
            w=MagicMock(),
        )

    out = capsys.readouterr().out
    assert "GSO_PLAN11_DISPATCH_DECISION_V1" in out
    assert '"outcome":"entered"' in out, (
        f"Expected outcome=entered; got captured output:\n{out}"
    )
    assert "build_failing_qids_empty" not in out, (
        "Step 1.3.5 closed the build_failing_qids_empty path for the "
        "typical-production case (Plan 11 ON, no typed evidence, but "
        "failing rows in eval_results)"
    )
