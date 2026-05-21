"""Plan 12 — verify the dispatch split in optimizer.py.

When plan11_llm_first_enabled() is True and rca_evidence_typed is
EMPTY, the new code path must still attempt Plan 11 via
_build_plan11_failing_qids_from_raw. Verified by patching the
two Plan 11 stage modules and asserting they are called.
"""
from unittest.mock import MagicMock, patch


def _make_metadata_snapshot():
    return {
        "iteration": 0,
        "optimization_run_id": "run_x",
        "schema_columns": ["catalog.schema.orders.customer_id"],
        "_eval_rows_failing": [
            {
                "question_id": "gs_009",
                "question": "Top 10 customers",
                "ground_truth_sql": "GT",
                "generated_sql": "GEN",
                "judge_rationale": "RANK without LIMIT",
                "score": 0.0,
            },
        ],
    }


def test_plan11_runs_when_typed_evidence_empty_but_flag_on(
    capsys, monkeypatch,
):
    """The Plan 12 dispatch split MUST call diagnose_failing_qids even
    when rca_evidence_typed is empty, as long as plan11_llm_first
    is on and there are failing qids."""
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.optimizer import (
        _decide_and_run_plan11_dispatch,
    )

    metadata_snapshot = _make_metadata_snapshot()
    diagnose_called: list[bool] = []
    cluster_called: list[bool] = []

    def fake_diagnose(**kwargs):
        diagnose_called.append(True)
        return []  # Stage 1 returns no diagnoses — Stage 2 should not run

    def fake_cluster(**kwargs):
        cluster_called.append(True)
        return []

    # The dispatch helper imports diagnose_failing_qids /
    # cluster_diagnoses lazily inside its body. Patching the module
    # attribute reaches both binding paths.
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )
    with patch.object(
        _stage1_mod, "diagnose_failing_qids", side_effect=fake_diagnose,
    ), patch.object(
        _stage2_mod, "cluster_diagnoses", side_effect=fake_cluster,
    ):
        result = _decide_and_run_plan11_dispatch(
            failing_qids=["gs_009"],
            rca_evidence_typed={},
            metadata_snapshot=metadata_snapshot,
            namespace="hard",
            signal_type="hard",
            run_id="run_x",
            w=None,
        )

    assert diagnose_called == [True], (
        "Stage 1 must run even when rca_evidence_typed is empty"
    )
    captured = capsys.readouterr().out
    assert "GSO_PLAN11_DISPATCH_DECISION_V1" in captured
    assert '"outcome":"entered"' in captured


def test_plan11_emits_skipped_marker_when_flag_off(capsys, monkeypatch):
    """When the flag is off, the dispatch helper must emit a 'skipped'
    decision marker with reason='flag_disabled' before returning None."""
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "0")
    from genie_space_optimizer.optimization.optimizer import (
        _decide_and_run_plan11_dispatch,
    )

    result = _decide_and_run_plan11_dispatch(
        failing_qids=["gs_009"],
        rca_evidence_typed={},
        metadata_snapshot=_make_metadata_snapshot(),
        namespace="hard",
        signal_type="hard",
        run_id="run_x",
        w=None,
    )

    assert result is None
    captured = capsys.readouterr().out
    assert "GSO_PLAN11_DISPATCH_DECISION_V1" in captured
    assert '"outcome":"skipped"' in captured
    assert '"skip_reason":"flag_disabled"' in captured


def test_plan11_emits_skipped_marker_when_no_failing_qids(
    capsys, monkeypatch,
):
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")
    from genie_space_optimizer.optimization.optimizer import (
        _decide_and_run_plan11_dispatch,
    )

    result = _decide_and_run_plan11_dispatch(
        failing_qids=[],
        rca_evidence_typed={},
        metadata_snapshot=_make_metadata_snapshot(),
        namespace="hard",
        signal_type="hard",
        run_id="run_x",
        w=None,
    )

    assert result is None
    captured = capsys.readouterr().out
    assert '"skip_reason":"no_failing_qids"' in captured
