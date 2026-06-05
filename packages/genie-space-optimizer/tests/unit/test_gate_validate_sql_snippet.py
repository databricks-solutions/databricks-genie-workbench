"""Phase 3 P3.1 — single sql-snippet validation gate.

Pins:

  * The gate is a no-op for proposals whose patch_type is not in
    ``_SQL_ARMS`` (e.g. ``add_instruction``).
  * When the canonical validator passes, the gate emits a success
    verdict and stamps the result on ``ctx.extras`` keyed by
    ``(intent_id, body_field)``.
  * When the canonical validator fails, the gate emits a
    ``reject_terminal`` verdict with a typed forbidden_signature of
    the form ``snippet_validation_failed:<patch_type>:<reason>``.
  * Legacy bypass call sites can consult the verdict via
    :func:`lookup_sql_snippet_verdict`.
"""
from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformers import (
    gate_validate_sql_snippet as gate_module,
)
from genie_space_optimizer.optimization.state_machine.transformers.gate_validate_sql_snippet import (
    gate_validate_sql_snippet,
    lookup_sql_snippet_verdict,
)


def _proposal(
    *,
    patch_type: PatchType,
    patch_body: dict[str, Any],
    intent_id: str = "i1",
) -> RepairProposal:
    return RepairProposal(
        intent_id=intent_id,
        intent_name="n",
        intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=patch_type,
        rationale="r",
        confidence="medium",
        patch_body=patch_body,
        blame_set=(),
    )


class _FakeStore:
    def __init__(self, by_id: dict[str, RepairProposal]) -> None:
        self._by_id = by_id

    def lookup(self, intent_id: str) -> RepairProposal | None:
        return self._by_id.get(intent_id)


def _state_with_proposal(intent_id: str, patch_type: PatchType) -> QuestionStateInIteration:
    seen = HardQidSeenRecord(
        eval_row_id="er_1",
        predicate="row_is_hard_failure",
        score=0.0,
        baseline_sql="SELECT 1",
        expected_shape="aggregate",
        iteration_first_seen=1,
    )
    state = QuestionStateInIteration(
        qid="qid_1",
        iteration=1,
        current_stage=FunnelStage.APPLYABLE,
        deepest_stage_reached=FunnelStage.APPLYABLE,
        seen=seen,
        proposals=(
            ProposalAttempt(
                attempt_index=0,
                intent_id=intent_id,
                patch_type=str(patch_type.value),
                deepest_stage_in_attempt=FunnelStage.APPLYABLE,
                outcome="applied",
                outcome_reason="",
            ),
        ),
        transitions=(
            StageTransition(
                from_stage=FunnelStage.HARD_QID_SEEN,
                to_stage=FunnelStage.APPLYABLE,
                at_ms=0,
                transformer_name="seed",
                transition_kind="validation_gate",
            ),
        ),
    )
    return state


def _ctx(
    *,
    proposal: RepairProposal,
    extras: dict[str, Any] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        proposal_store=_FakeStore({proposal.intent_id: proposal}),
        metadata_snapshot={},
        spark=None,
        catalog="",
        gold_schema="",
        w=None,
        warehouse_id="",
        extras=extras if extras is not None else {},
    )


def test_gate_is_noop_for_non_sql_patch_types() -> None:
    p = _proposal(
        patch_type=PatchType.ADD_INSTRUCTION,
        patch_body={"instruction_text": "be careful"},
    )
    state = _state_with_proposal(p.intent_id, p.patch_type)
    ctx = _ctx(proposal=p)
    verdict = gate_module._predicate(state, ctx)
    assert verdict.passed is True


def test_gate_is_noop_when_sql_field_blank() -> None:
    p = _proposal(
        patch_type=PatchType.ADD_SQL_SNIPPET_FILTER,
        patch_body={"name": "f", "sql_expression": ""},
    )
    state = _state_with_proposal(p.intent_id, p.patch_type)
    ctx = _ctx(proposal=p)
    verdict = gate_module._predicate(state, ctx)
    assert verdict.passed is True


def test_gate_passes_through_when_canonical_validator_passes(
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_canonical(*, sql, snippet_type, **_kw):
        captured["sql"] = sql
        captured["snippet_type"] = snippet_type
        return (True, sql.strip())

    monkeypatch.setattr(
        gate_module, "canonical_validate_sql_snippet", _fake_canonical
    )
    p = _proposal(
        patch_type=PatchType.ADD_SQL_SNIPPET_FILTER,
        patch_body={
            "name": "active_region",
            "sql_expression": "region = 'WEST'",
        },
    )
    state = _state_with_proposal(p.intent_id, p.patch_type)
    extras: dict[str, Any] = {}
    ctx = _ctx(proposal=p, extras=extras)
    verdict = gate_module._predicate(state, ctx)
    assert verdict.passed is True
    bucket = extras["_p3_1_validate_sql_snippet_results"]
    assert bucket[(p.intent_id, "sql_expression")] == (True, "region = 'WEST'")
    assert captured["snippet_type"] == "filters"


def test_gate_terminates_with_typed_signature_on_validator_failure(
    monkeypatch,
) -> None:
    def _fake_canonical(*, sql, snippet_type, **_kw):
        del sql, snippet_type
        return (False, "unresolved_column:foo.bar")

    monkeypatch.setattr(
        gate_module, "canonical_validate_sql_snippet", _fake_canonical
    )
    p = _proposal(
        patch_type=PatchType.ADD_SQL_SNIPPET_EXPRESSION,
        patch_body={"name": "x", "sql_expression": "SUM(foo.bar)"},
    )
    state = _state_with_proposal(p.intent_id, p.patch_type)
    extras: dict[str, Any] = {}
    ctx = _ctx(proposal=p, extras=extras)
    verdict = gate_module._predicate(state, ctx)
    assert verdict.passed is False
    rejection = verdict.rejection_outcome
    assert isinstance(rejection, TerminalRecord)
    assert rejection.kind == "OPTIMIZER_STALLED_SAFE_NOOP"
    assert rejection.reason.startswith("snippet_validation_failed:")
    assert (
        rejection.forbidden_signature
        == "snippet_validation_failed:add_sql_snippet_expression:unresolved_column:foo.bar"
    )
    bucket = extras["_p3_1_validate_sql_snippet_results"]
    assert bucket[(p.intent_id, "sql_expression")] == (
        False,
        "unresolved_column:foo.bar",
    )


def test_lookup_helper_returns_none_when_gate_did_not_run() -> None:
    extras: dict[str, Any] = {}
    ctx = SimpleNamespace(extras=extras)
    assert lookup_sql_snippet_verdict(ctx, "i", "example_sql") is None


def test_lookup_helper_round_trip() -> None:
    extras: dict[str, Any] = {
        "_p3_1_validate_sql_snippet_results": {
            ("i", "example_sql"): (True, "SELECT 1"),
        }
    }
    ctx = SimpleNamespace(extras=extras)
    assert lookup_sql_snippet_verdict(ctx, "i", "example_sql") == (
        True,
        "SELECT 1",
    )


def test_public_transformer_routes_apply_able_to_applyable() -> None:
    assert gate_validate_sql_snippet.from_stage is FunnelStage.APPLYABLE
    assert gate_validate_sql_snippet.to_stage_on_success is FunnelStage.APPLYABLE
    assert gate_validate_sql_snippet.to_stage_on_reject is FunnelStage.TERMINATED


def test_gate_handles_add_example_sql_negative_arm(monkeypatch) -> None:
    """P2.4 negative variant must route through the same gate."""
    captured: dict[str, Any] = {}

    def _fake_canonical(*, sql, snippet_type, **_kw):
        captured["snippet_type"] = snippet_type
        return (True, sql.strip())

    monkeypatch.setattr(
        gate_module, "canonical_validate_sql_snippet", _fake_canonical
    )
    p = _proposal(
        patch_type=PatchType.ADD_EXAMPLE_SQL_NEGATIVE,
        patch_body={
            "question": "How many?",
            "example_sql": "SELECT COUNT(*) FROM t",
        },
    )
    state = _state_with_proposal(p.intent_id, p.patch_type)
    extras: dict[str, Any] = {}
    ctx = _ctx(proposal=p, extras=extras)
    verdict = gate_module._predicate(state, ctx)
    assert verdict.passed is True
    assert captured["snippet_type"] == "example_sql"
