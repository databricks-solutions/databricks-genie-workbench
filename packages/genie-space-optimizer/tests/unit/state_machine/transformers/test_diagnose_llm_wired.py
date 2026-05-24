"""Step §B of the production-seam wire-in plan.

``_invoke_stage1_llm`` now adapts ``stages.diagnose.diagnose_failing_qids``
into the ``LlmReasoningResponse``-shaped object the transformer expects.
Tests monkeypatch ``diagnose_failing_qids`` at the adapter callsite
inside ``state_machine.transformers.diagnose_llm`` so we exercise the
adapter logic itself.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformers import (
    diagnose_llm as diagnose_module,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _make_state(qid: str = "q1") -> QuestionStateInIteration:
    seen = HardQidSeenRecord(
        eval_row_id="er_1",
        predicate="row_is_hard_failure",
        score=0.0,
        baseline_sql="SELECT 1",
        expected_shape="aggregate",
        iteration_first_seen=1,
    )
    return QuestionStateInIteration(
        qid=qid,
        iteration=1,
        current_stage=FunnelStage.HARD_QID_SEEN,
        deepest_stage_reached=FunnelStage.HARD_QID_SEEN,
        seen=seen,
        diagnosed=None,
        clustered=None,
        proposals=(),
        applied=None,
        evaluated=None,
        accepted=None,
        terminal=None,
        transitions=(
            StageTransition(
                from_stage=FunnelStage.HARD_QID_SEEN,
                to_stage=FunnelStage.HARD_QID_SEEN,
                at_ms=0,
                transformer_name="dispatch_input",
                transition_kind="ingest",
            ),
        ),
    )


def _make_ctx(
    *,
    baseline_eval_rows: tuple = (),
    schema_columns: tuple = ("orders.status",),
) -> TransformerContext:
    return TransformerContext(
        iteration=1,
        run_id="run_x",
        validation_context=ValidationContext(1, "run_x", {}),
        schema_columns=schema_columns,
        baseline_eval_rows=baseline_eval_rows,
        w=None,
    )


def test_happy_path_advances_to_diagnosed(monkeypatch):
    """When diagnose_failing_qids returns a matching PerQidDiagnosis,
    the transformer advances HARD_QID_SEEN → DIAGNOSED."""
    from genie_space_optimizer.optimization.stages.plan11_types import (
        PerQidDiagnosis,
    )

    state = _make_state("q1")
    ctx = _make_ctx(
        baseline_eval_rows=(
            {
                "question_id": "q1",
                "question": "How many?",
                "ground_truth_sql": "SELECT COUNT(*) FROM t",
                "generated_sql": "SELECT 1",
                "judge_rationale": "wrong aggregate",
                # Trial 12 Stage1InputEvidenceContract: blame_set_seed
                # and rca_evidence.* are populated from ASI metadata
                # by build_stage1_evidence_card; supply minimal ASI
                # so the contract pre-flight does not skip the call.
                "feedback/asi/metadata": {
                    "failure_type": "missing_filter",
                    "wrong_clause": "WHERE",
                    "blame_set": ["orders.status"],
                    "counterfactual_fix": "add WHERE clause",
                    "patch_family": "add_sql_snippet_expression",
                    "rca_kind": "filter_dropped",
                },
            },
        ),
    )

    captured = {}

    def fake_diagnose(
        *, failing_qids, schema_columns, optimization_run_id,
        iteration, w, recent_diagnoses=None,
    ):
        captured["failing_qids"] = failing_qids
        captured["schema_columns"] = schema_columns
        return [
            PerQidDiagnosis(
                qid="q1",
                rca_kind_label="missing_filter",
                observed_failure="returns all rows",
                generated_sql_issue="no WHERE",
                expected_sql_shape="aggregate with filter",
                blame_set=("orders.status",),
                evidence_summary="judge said wrong aggregate",
                confidence="high",
            ),
        ]

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.diagnose.diagnose_failing_qids",
        fake_diagnose,
    )

    new_state = diagnose_module.plan11_stage1_diagnosis.transform(state, ctx)

    assert new_state.current_stage == FunnelStage.DIAGNOSED
    assert new_state.diagnosed is not None
    assert new_state.diagnosed.rca_kind_label == "missing_filter"
    assert new_state.diagnosed.evidence_summary == "judge said wrong aggregate"
    assert new_state.diagnosed.expected_sql_shape == "aggregate with filter"
    assert new_state.diagnosed.confidence == "high"
    # rca_card_id is synthesized deterministically from qid + label.
    assert "q1" in new_state.diagnosed.rca_card_id
    assert "missing_filter" in new_state.diagnosed.rca_card_id
    # Adapter passed only this state's QID as a single-element batch.
    assert len(captured["failing_qids"]) == 1
    assert captured["failing_qids"][0]["qid"] == "q1"
    assert captured["schema_columns"] == ["orders.status"]


def test_decline_when_diagnose_returns_empty(monkeypatch):
    """When diagnose_failing_qids returns [] (LLM declined or empty),
    the transformer terminates with OPTIMIZER_NO_CANDIDATES."""
    state = _make_state("q1")
    ctx = _make_ctx(
        baseline_eval_rows=(
            {
                "question_id": "q1",
                "question": "How many?",
                "ground_truth_sql": "x", "generated_sql": "y",
                "judge_rationale": "z",
            },
        ),
    )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.diagnose.diagnose_failing_qids",
        lambda **kw: [],
    )

    new_state = diagnose_module.plan11_stage1_diagnosis.transform(state, ctx)

    assert new_state.current_stage == FunnelStage.TERMINATED
    assert new_state.terminal is not None
    assert new_state.terminal.kind == "OPTIMIZER_NO_CANDIDATES"


def test_decline_when_no_matching_eval_row(monkeypatch):
    """When the state's qid is not in ctx.baseline_eval_rows we cannot
    build the diagnose payload — terminate cleanly rather than calling
    the LLM with garbage."""
    state = _make_state("q_missing")
    ctx = _make_ctx(baseline_eval_rows=())  # empty rows

    called = {"count": 0}

    def fake_diagnose(**kw):
        called["count"] += 1
        return []

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.diagnose.diagnose_failing_qids",
        fake_diagnose,
    )

    new_state = diagnose_module.plan11_stage1_diagnosis.transform(state, ctx)

    assert new_state.current_stage == FunnelStage.TERMINATED
    # The diagnose call should be short-circuited — no LLM call burned.
    assert called["count"] == 0


def test_decline_when_no_diagnosis_matches_qid(monkeypatch):
    """Diagnose returned diagnoses but none for our QID — defensive
    contract failure: terminate with OPTIMIZER_NO_CANDIDATES."""
    from genie_space_optimizer.optimization.stages.plan11_types import (
        PerQidDiagnosis,
    )

    state = _make_state("q1")
    ctx = _make_ctx(
        baseline_eval_rows=(
            {
                "question_id": "q1",
                "question": "?", "ground_truth_sql": "x",
                "generated_sql": "y", "judge_rationale": "z",
            },
        ),
    )

    def fake_diagnose(**kw):
        # LLM hallucinated a qid we didn't ask about.
        return [
            PerQidDiagnosis(
                qid="someone_else",
                rca_kind_label="other",
                observed_failure="",
                generated_sql_issue="",
                expected_sql_shape="",
                blame_set=(),
                evidence_summary="",
                confidence="low",
            ),
        ]

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.diagnose.diagnose_failing_qids",
        fake_diagnose,
    )

    new_state = diagnose_module.plan11_stage1_diagnosis.transform(state, ctx)
    assert new_state.current_stage == FunnelStage.TERMINATED
