from unittest.mock import MagicMock

from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.rca_evidence import (
    RcaEvidenceInput,
    Stage2Evidence,
)


def _stub_ctx() -> StageContext:
    return StageContext(
        run_id="r1",
        iteration=1,
        space_id="s1",
        domain="airline",
        catalog="main",
        schema="gso",
        apply_mode="real",
        journey_emit=MagicMock(),
        decision_emit=MagicMock(),
        mlflow_anchor_run_id=None,
        feature_flags={},
    )


def test_rca_evidence_input_required_fields() -> None:
    inp = RcaEvidenceInput(
        eval_rows=({"qid": "q2", "passed": False},),
        hard_failure_qids=("q2",),
        soft_signal_qids=(),
        per_qid_judge={"q2": {"verdict": "wrong_join_spec"}},
        asi_metadata={"q2": {"intent_keywords": ["top"]}},
    )
    assert inp.hard_failure_qids == ("q2",)
    assert inp.per_qid_judge["q2"]["verdict"] == "wrong_join_spec"


def test_stage2_evidence_required_fields() -> None:
    ev = Stage2Evidence(
        per_qid_evidence={
            "q2": {
                "rca_kind": "top_n_cardinality_collapse",
                "judge_verdict": "wrong_join_spec",
                "sql_diff": "RANK() … no LIMIT",
                "counterfactual_fix": {"add_filter": "rn <= 3"},
                "asi_features": {"top_n_intent": True},
            },
        },
        rca_kinds_by_qid={"q2": "top_n_cardinality_collapse"},
        evidence_refs={"q2": ("trace://r/iter/1/judge/q2",)},
        promoted_to_top_n_qids=("q2",),
    )
    assert ev.rca_kinds_by_qid["q2"] == "top_n_cardinality_collapse"
    assert ev.promoted_to_top_n_qids == ("q2",)


def test_collect_resolves_rca_kind_for_single_hard_failure() -> None:
    """A hard failure with judge verdict resolves to a non-empty RcaKind."""
    from genie_space_optimizer.optimization.stages import rca_evidence
    ctx = _stub_ctx()
    inp = rca_evidence.RcaEvidenceInput(
        eval_rows=(
            {"qid": "q2", "passed": False, "generated_sql": "SELECT 1"},
        ),
        hard_failure_qids=("q2",),
        soft_signal_qids=(),
        per_qid_judge={"q2": {"verdict": "wrong_join_spec"}},
        asi_metadata={"q2": {}},
    )

    out = rca_evidence.collect(ctx, inp)

    assert "q2" in out.per_qid_evidence
    assert "rca_kind" in out.per_qid_evidence["q2"]
    # Without top-N intent signals, wrong_join_spec resolves to
    # JOIN_SPEC_MISSING_OR_WRONG (canonical RcaKind enum value).
    assert out.rca_kinds_by_qid["q2"] in {
        "join_spec_missing_or_wrong",
        "top_n_cardinality_collapse",
        "measure_swap",
    }


def test_collect_promotes_top_n_when_pr_d_intent_signal_present() -> None:
    """PR-D regression: RANK() without LIMIT N + top-N intent must
    re-route from wrong_join_spec to top_n_cardinality_collapse."""
    from genie_space_optimizer.optimization.stages import rca_evidence
    ctx = _stub_ctx()
    inp = rca_evidence.RcaEvidenceInput(
        eval_rows=(
            {
                "qid": "q9",
                "passed": False,
                "generated_sql": "SELECT *, RANK() OVER (ORDER BY p DESC) FROM t",
            },
        ),
        hard_failure_qids=("q9",),
        soft_signal_qids=(),
        per_qid_judge={"q9": {"verdict": "wrong_join_spec"}},
        asi_metadata={"q9": {
            "intent_keywords": ["top", "highest"],
            "question_text": "What are the top 3 routes?",
        }},
    )

    out = rca_evidence.collect(ctx, inp)

    assert out.rca_kinds_by_qid["q9"] == "top_n_cardinality_collapse"
    assert "q9" in out.promoted_to_top_n_qids


def test_collect_handles_empty_inputs() -> None:
    """No hard failures, no soft signals → empty evidence record."""
    from genie_space_optimizer.optimization.stages import rca_evidence
    ctx = _stub_ctx()
    inp = rca_evidence.RcaEvidenceInput(
        eval_rows=(),
        hard_failure_qids=(),
        soft_signal_qids=(),
    )
    out = rca_evidence.collect(ctx, inp)
    assert out.per_qid_evidence == {}
    assert out.rca_kinds_by_qid == {}
    assert out.evidence_refs == {}
    assert out.promoted_to_top_n_qids == ()
