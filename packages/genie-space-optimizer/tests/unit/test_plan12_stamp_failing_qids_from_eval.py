"""Plan 12 Step 1.3.5 — cluster_failures must stamp _failing_qids and
_eval_rows_failing on metadata_snapshot from the eval_results when
the iteration scope didn't pre-stamp them. This closes the
"build_failing_qids_empty" decline path the dispatch helper
previously emitted when typed evidence was empty AND no metadata
was stamped — the typical production case.

2026-05-22 update: the stamping predicate now delegates to
:func:`evaluation.row_is_hard_failure` (rc=no AND arbiter not in
correct-verdicts), matching the canonical predicate the accuracy
gate and legacy clustering use. The row shape these tests construct
mirrors the production replay-fixture shape: ``arbiter`` +
``question_id`` + ``result_correctness``. No ``score`` field.
"""
from unittest.mock import MagicMock, patch


def _eval_results_with_rows(rows):
    """Plain-dict eval_results shape (the most common path)."""
    return {"rows": rows}


def _hard(qid: str) -> dict:
    """Production-shaped row that the canonical predicate classifies as hard."""
    return {
        "question_id": qid,
        "result_correctness": "no",
        "arbiter": "ground_truth_correct",
    }


def _passing(qid: str) -> dict:
    return {
        "question_id": qid,
        "result_correctness": "yes",
        "arbiter": "both_correct",
    }


def _soft_arbiter_override(qid: str) -> dict:
    """rc=no but arbiter overrides to correct — stays soft."""
    return {
        "question_id": qid,
        "result_correctness": "no",
        "arbiter": "both_correct",
    }


def test_stamp_failing_qids_from_dict_eval_results(monkeypatch):
    """The plain-dict eval_results.rows path: cluster_failures should
    extract canonical hard rows (rc=no AND arbiter not in correct set)
    and stamp them on metadata_snapshot before the Plan 11 dispatch
    runs."""
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    metadata_snapshot: dict = {"iteration": 1, "optimization_run_id": "run_x"}

    eval_results = _eval_results_with_rows([
        _hard("gs_009"),
        _hard("gs_021"),
        _passing("gs_007"),
        _soft_arbiter_override("gs_011"),  # rc=no but arbiter=both_correct
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
            eval_results=_eval_results_with_rows([_hard("gs_009")]),
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


def test_stamp_handles_missing_correctness_fields(monkeypatch):
    """Rows missing ``result_correctness`` should NOT be stamped as
    hard. Under the canonical predicate, a row with no rc evidence is
    not classifiable as hard — the optimizer must require a judge
    verdict, not infer one from absence of data.

    This is a deliberate semantic change from the pre-2026-05-22
    behavior, which defaulted missing scores to ``0.0 < 0.5 → hard``.
    That defensive over-counting was actively wrong: it masked the
    real ``failing_qids_count=0`` bug (production rows don't carry
    ``score`` at all) by sometimes inflating the count to a non-zero
    value driven by malformed data.
    """
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    metadata_snapshot: dict = {"iteration": 1, "optimization_run_id": "run_x"}

    eval_results = _eval_results_with_rows([
        {"question_id": "gs_unknown_a"},  # no rc, no arbiter
        {"question_id": "gs_unknown_b", "result_correctness": ""},  # empty
        _passing("gs_passing"),  # rc=yes, arbiter=both_correct
        _hard("gs_real_hard"),  # canonical hard
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

    # Only the canonical hard row is stamped.
    assert metadata_snapshot["_failing_qids"] == ["gs_real_hard"], (
        "Missing/empty rc fields are not hard; only rc=no AND arbiter "
        f"not-correct qualifies. Got {metadata_snapshot.get('_failing_qids')!r}"
    )


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
            eval_results=_eval_results_with_rows([_hard("gs_009")]),
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
