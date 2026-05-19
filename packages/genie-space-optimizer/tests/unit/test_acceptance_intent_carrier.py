"""Plan 1 Task 10 — AgOutcome.intent_outcomes_by_id carrier.

After acceptance decides per-AG outcomes, every typed AppliedPatch
with a non-empty intent_id gets a typed IntentOutcome carrying the
outcome string, applied_signature, applied_at_iter, and rollback
reason (when applicable).
"""

from __future__ import annotations

from types import SimpleNamespace

from genie_space_optimizer.optimization.repair_intent import IntentOutcome
from genie_space_optimizer.optimization.stages.acceptance import (
    AcceptanceInput,
    AgOutcome,
    AgOutcomeRecord,
    decide,
)


def _ctx():
    emitted: list = []
    return SimpleNamespace(
        run_id="run",
        iteration=1,
        decision_emit=lambda r: emitted.append(r),
        _emitted=emitted,
    )


def test_ag_outcome_record_has_intent_outcomes_field() -> None:
    rec = AgOutcomeRecord(ag_id="AG_X", outcome="accepted", reason_code="ok")
    assert hasattr(rec, "intent_outcomes")
    assert rec.intent_outcomes == ()


def test_ag_outcome_has_intent_outcomes_by_id_carrier() -> None:
    out = AgOutcome()
    assert hasattr(out, "intent_outcomes_by_id")
    assert out.intent_outcomes_by_id == {}


def test_decide_produces_intent_outcome_for_each_intent_id() -> None:
    inp = AcceptanceInput(
        applied_entries_by_ag={
            "AG_X": (
                {
                    "patch": {
                        "proposal_id": "p1",
                        "patch_type": "add_example_sql",
                        "target_qids": ["gs_009"],
                        "intent_id": "i1",
                        "content_fingerprint": "fp1",
                    }
                },
            )
        },
        ags=({"id": "AG_X", "target_qids": ("gs_009",)},),
        baseline_accuracy=0.5,
        candidate_accuracy=0.6,
        pre_rows=({"question_id": "gs_009", "result_correctness": "no"},),
        post_rows=({"question_id": "gs_009", "result_correctness": "yes"},),
        min_gain_pp=0.0,
    )
    out = decide(_ctx(), inp)
    assert "i1" in out.intent_outcomes_by_id
    intent_outcome = out.intent_outcomes_by_id["i1"]
    assert isinstance(intent_outcome, IntentOutcome)
    assert intent_outcome.ag_id == "AG_X"
    assert intent_outcome.outcome in {
        "accepted",
        "accepted_with_regression_debt",
        "accepted_with_attribution_drift",
    }


def test_decide_marks_rolled_back_intent_with_outcome_string() -> None:
    inp = AcceptanceInput(
        applied_entries_by_ag={
            "AG_X": (
                {
                    "patch": {
                        "proposal_id": "p1",
                        "patch_type": "add_example_sql",
                        "target_qids": ["gs_009"],
                        "intent_id": "i1",
                        "content_fingerprint": "fp1",
                    }
                },
            )
        },
        ags=({"id": "AG_X", "target_qids": ("gs_009",)},),
        baseline_accuracy=0.6,
        candidate_accuracy=0.5,
        pre_rows=({"question_id": "gs_009", "result_correctness": "yes"},),
        post_rows=({"question_id": "gs_009", "result_correctness": "no"},),
        min_gain_pp=0.0,
    )
    out = decide(_ctx(), inp)
    intent_outcome = out.intent_outcomes_by_id["i1"]
    assert intent_outcome.outcome == "rolled_back"


def test_decide_skips_intent_outcome_when_no_intent_id_on_patch() -> None:
    """Legacy / unstamped patches don't appear in the typed carrier."""
    inp = AcceptanceInput(
        applied_entries_by_ag={
            "AG_X": (
                {
                    "patch": {
                        "proposal_id": "p_legacy",
                        "patch_type": "add_example_sql",
                        "target_qids": ["gs_009"],
                        "content_fingerprint": "fp1",
                    }
                },
            )
        },
        ags=({"id": "AG_X", "target_qids": ("gs_009",)},),
        baseline_accuracy=0.5,
        candidate_accuracy=0.6,
        pre_rows=({"question_id": "gs_009", "result_correctness": "no"},),
        post_rows=({"question_id": "gs_009", "result_correctness": "yes"},),
        min_gain_pp=0.0,
    )
    out = decide(_ctx(), inp)
    assert out.intent_outcomes_by_id == {}


def test_ag_outcome_round_trip_preserves_carriers() -> None:
    intent_outcome = IntentOutcome(
        intent_id="i1",
        ag_id="AG_X",
        outcome="accepted",
        applied_signature="sig",
        applied_at_iter=1,
        rollback_reason=None,
    )
    out = AgOutcome(
        outcomes_by_ag={
            "AG_X": AgOutcomeRecord(
                ag_id="AG_X",
                outcome="accepted",
                reason_code="ok",
                intent_outcomes=(intent_outcome,),
            ),
        },
        intent_outcomes_by_id={"i1": intent_outcome},
    )
    restored = AgOutcome.from_json(out.to_json())
    assert restored.intent_outcomes_by_id["i1"] == intent_outcome
    assert restored.outcomes_by_ag["AG_X"].intent_outcomes[0] == intent_outcome
