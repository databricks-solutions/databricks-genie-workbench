"""Historical-bug retroactive validation — v1.7 chunk 6.

The proof that the fuzzer is a *real* bug-finder: take each known
Trial 16 root cause, revert the fix via monkeypatch, run a fixture
through the workbench, and assert the relevant invariant flags the
regression. If any of these tests stop catching their respective
regression, the corresponding invariant has drifted and needs
strengthening before the fuzzer can be trusted to catch future
regressions of the same shape.

Three scenarios:

* **RC2 — acceptance_gate forbidden_signature wiring**: Trial 16
  Chunk 3 populated ``forbidden_signature`` on the
  ``acceptance_gate`` ``OPTIMIZER_TRIED_NO_GAIN`` terminal. Reverting
  to ``forbidden_signature=""`` must trip the B2 invariant
  (``validation_gate_forbidden_signature_nonempty``) and the E1
  invariant (``gate_terminal_carries_signature``).
* **RC2 — evaluated_gate forbidden_signature wiring**: Trial 16
  populated ``forbidden_signature`` on the ``evaluated_gate``
  ``OPTIMIZER_INVARIANT_VIOLATION`` terminal. Reverting must trip
  B2/E1 on the post-apply-eval-failure path.
* **RC3 — applier_gate ``to_stage_on_reject`` routing**: Trial 16
  flipped this from ``FunnelStage.PROPOSED`` (which looped no-op
  rejections) to ``FunnelStage.TERMINATED``. Reverting must trip A1
  (``single_terminal_stage``) because the QID never reaches a
  terminal state in one iteration.
"""
from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Iterable

import pytest

from genie_space_optimizer.optimization.state_machine.transformers import (
    acceptance_gate as acceptance_gate_module,
    applier_gate as applier_gate_module,
    evaluated_gate as evaluated_gate_module,
)
from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
from genie_space_optimizer.optimization.state_machine.records import (
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.transformer import (
    GateVerdict,
    ValidationGate,
)
from local_lever_workbench.fuzzer import check_all_invariants
from local_lever_workbench.input_bundle import from_production_replay
from local_lever_workbench.local_runner import (
    LLM_MODE_TAPE,
    run_workbench_iteration,
)
from local_lever_workbench.models import (
    WorkbenchInputBundle,
    WorkbenchRunConfig,
)


_QID = ("gs_009", "domain_a_gs_009")


def _serialize_tape(entries: Iterable, path: Path) -> Path:
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "kind": e.kind,
                    "skill_id": e.skill_id,
                    "call_id": e.call_id,
                    "iteration": e.iteration,
                    "qid": e.qid,
                    "parsed_output": e.parsed_output,
                    "raw_text": e.raw_text,
                    "tokens_input": e.tokens_input,
                    "tokens_output": e.tokens_output,
                    "duration_ms": e.duration_ms,
                    "exception_class": e.exception_class,
                    "exception_message": e.exception_message,
                }
            )
            for e in entries
        )
    )
    return path


def _full_pipeline_tape(qids: tuple[str, ...], tmp_path: Path) -> Path:
    from tests.integration.sm_forward_tapes import (
        cluster_response_tape,
        diagnose_response_tape,
        synthesize_response_tape,
    )
    tape: list = []
    for _ in range(5):
        tape += diagnose_response_tape(qids)
    for _ in range(5):
        tape += cluster_response_tape(qids)
    for _ in range(5):
        tape += synthesize_response_tape(qids)
    return _serialize_tape(tape, tmp_path / "forward.jsonl")


def _bundle_rolled_back() -> WorkbenchInputBundle:
    suffix, canonical = _QID
    base = from_production_replay(qids=(suffix,))
    return WorkbenchInputBundle(
        provenance=base.provenance,
        space_id=base.space_id,
        hard_cases=base.hard_cases,
        metadata_snapshot=base.metadata_snapshot,
        # Score 0.0 == baseline; acceptance_gate rolls back with
        # ``target_unchanged: post_score <= pre_score``.
        post_apply_eval_tape=(
            {
                "question_id": canonical,
                "inputs/question_id": canonical,
                "generated_sql": f"SELECT POST -- {canonical}",
                "feedback/result_correctness/value": 0.0,
                "eval_row_id": f"workbench-hist-{canonical}",
            },
        ),
    )


def _run(bundle: WorkbenchInputBundle, tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    tape_path = _full_pipeline_tape(bundle.hard_qids, tmp_path)
    config = WorkbenchRunConfig(
        bundle_path=tmp_path / "bundle.json",
        output_dir=tmp_path / "out",
        llm_mode=LLM_MODE_TAPE,
        apply_mode="fake-record",
        tape_path=tape_path,
        iteration=1,
    )
    return run_workbench_iteration(bundle, config)


# ─── RC2 — acceptance_gate forbidden_signature regression ───────────


@pytest.mark.workbench
@pytest.mark.integration
def test_fuzzer_flags_acceptance_gate_empty_signature_regression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Revert the acceptance_gate strategist-feedback wiring → fuzzer flags.

    Trial 18 (``GSO_TRIAL18_ACCEPTANCE_OVERHAUL``, default-ON) reshaped
    the no-behavioural-gain boundary the original RC2 fix protected.
    The ``post_score == pre_score`` / no-collateral case no longer
    terminates with an ``OPTIMIZER_TRIED_NO_GAIN`` ``forbidden_signature``
    terminal — it is kept live in the ``kept_insufficient`` lane (a
    SUCCESS verdict) and the strategist's cumulative-learning channel is
    now ``AcceptanceDecisionRecord.insufficient_repair_signature``,
    guarded by invariant **F1** (``kept_insufficient_emits_signature``).

    This test reverts that wiring — zeroing the
    ``insufficient_repair_signature`` on the kept_insufficient success —
    and asserts the fuzzer's F1 invariant flags the regression. (The
    sibling ``evaluated_gate`` / ``applier_gate`` tests below still
    cover the B2/E1 forbidden_signature invariants via forced terminals,
    so the terminal-signature guard remains exercised.)
    """
    # Verify baseline: with Trial 18 in place, invariants are clean.
    bundle = _bundle_rolled_back()
    baseline_artifacts = _run(bundle, tmp_path / "baseline")
    baseline_result = check_all_invariants(baseline_artifacts)
    assert baseline_result.ok, (
        f"baseline (Trial 18 active) should be clean; got: "
        f"{baseline_result.violations!r}"
    )

    # Revert: wrap the acceptance_gate predicate to zero out the
    # kept_insufficient lane's insufficient_repair_signature (the
    # Trial-18 successor to the forbidden_signature channel) while
    # still zeroing forbidden_signature on any genuine terminal.
    real_gate = acceptance_gate_module.acceptance_gate
    real_predicate = real_gate.predicate

    def predicate_with_empty_signature(state, ctx):  # type: ignore[no-untyped-def]
        verdict = real_predicate(state, ctx)
        rec = verdict.success_record
        if (
            verdict.passed
            and rec is not None
            and getattr(rec, "decision", "") == "kept_insufficient"
        ):
            return GateVerdict.success(record=dataclasses.replace(
                rec, insufficient_repair_signature="",
            ))
        outcome = verdict.rejection_outcome
        if isinstance(outcome, TerminalRecord):
            return GateVerdict.reject_terminal(dataclasses.replace(
                outcome, forbidden_signature="",
            ))
        return verdict

    reverted_gate = ValidationGate(
        name=real_gate.name,
        from_stage=real_gate.from_stage,
        to_stage_on_success=real_gate.to_stage_on_success,
        to_stage_on_reject=real_gate.to_stage_on_reject,
        predicate=predicate_with_empty_signature,
    )
    monkeypatch.setattr(
        acceptance_gate_module, "acceptance_gate", reverted_gate,
    )

    # Run the same fixture under the reverted gate.
    artifacts = _run(bundle, tmp_path / "reverted")
    result = check_all_invariants(artifacts)

    assert not result.ok, (
        "fuzzer should have flagged the acceptance_gate empty-signature "
        "regression but reported clean"
    )
    f1 = result.by_id("F1")
    assert f1, (
        f"expected F1 violation (kept_insufficient lane emitted empty "
        f"insufficient_repair_signature); got violations={result.violations!r}"
    )


# ─── RC2 — evaluated_gate forbidden_signature regression ────────────


@pytest.mark.workbench
@pytest.mark.integration
def test_fuzzer_flags_evaluated_gate_empty_signature_regression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Revert Trial 16's evaluated_gate signature wiring → fuzzer must flag.

    The ``evaluated_gate`` emits an ``OPTIMIZER_INVARIANT_VIOLATION``
    terminal when post-apply eval errors out. Trial 16 populated its
    ``forbidden_signature`` with ``f"post_apply_eval_failed:{exc}"``.
    Reverting must trip the same B2/E1 invariants, demonstrating the
    fuzzer catches the regression independent of which validation
    gate emitted it.

    We force the ``evaluated_gate`` terminal path by patching its
    predicate to raise unconditionally — same shape as the no-row-for-
    QID failure the postmortem captured.
    """
    bundle = _bundle_rolled_back()

    real_gate = evaluated_gate_module.evaluated_gate

    def predicate_with_forced_empty_terminal(state, ctx):  # type: ignore[no-untyped-def]
        # Always fail with an empty signature — same shape as the
        # pre-Trial-16 evaluated_gate when post-apply row was missing.
        return GateVerdict.reject_terminal(TerminalRecord(
            kind="OPTIMIZER_INVARIANT_VIOLATION",
            reason="post_apply_eval_failed:forced",
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature="",
        ))

    reverted_gate = ValidationGate(
        name=real_gate.name,
        from_stage=real_gate.from_stage,
        to_stage_on_success=real_gate.to_stage_on_success,
        to_stage_on_reject=real_gate.to_stage_on_reject,
        predicate=predicate_with_forced_empty_terminal,
    )
    monkeypatch.setattr(
        evaluated_gate_module, "evaluated_gate", reverted_gate,
    )

    artifacts = _run(bundle, tmp_path / "reverted")
    result = check_all_invariants(artifacts)

    assert not result.ok
    b2 = result.by_id("B2")
    assert b2, (
        f"expected B2 violation on evaluated_gate; got "
        f"violations={result.violations!r}"
    )
    # E1 cross-checks the rejected-gate marker stream, which the
    # monkeypatched predicate bypasses (marker emission lives in the
    # real predicate this test replaces). We still expect at least
    # one *other* downstream invariant to fire — either D1 (the
    # applied patch has no downstream verdict) or B1/A1. This proves
    # the regression surfaces in multiple invariants, not just B2.
    downstream = (
        result.by_id("D1") + result.by_id("A1") + result.by_id("B1")
    )
    assert downstream, (
        f"expected at least one downstream invariant (D1/A1/B1) to "
        f"also flag the evaluated_gate regression; got "
        f"violations={result.violations!r}"
    )


# ─── RC3 — applier_gate to_stage_on_reject routing regression ───────


@pytest.mark.workbench
@pytest.mark.integration
def test_fuzzer_flags_applier_gate_recycle_to_proposed_regression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Revert Trial 16 RC3 routing → fuzzer must flag A1.

    Trial 16 RC3 flipped ``applier_gate.to_stage_on_reject`` from
    ``FunnelStage.PROPOSED`` (which recycled no-op rejections in an
    infinite loop) to ``FunnelStage.TERMINATED``. Reverting routes
    rejected patches back to PROPOSED — in a single iteration the
    workbench then ends with the QID stranded at a non-terminal stage,
    which the fuzzer's A1 invariant must flag.

    We force the applier_gate to reject by patching its predicate to
    emit a ``reject_proposed`` outcome (same shape as the pre-Trial-16
    no-op-rejection path). The reverted gate routes to PROPOSED so
    the QID never reaches a terminal record in one iteration.
    """
    bundle = _bundle_rolled_back()

    from genie_space_optimizer.optimization.state_machine.records import (
        ProposalAttempt,
    )

    def predicate_always_recycles(state, ctx):  # type: ignore[no-untyped-def]
        return GateVerdict.reject_proposal(ProposalAttempt(
            attempt_index=0,
            intent_id="fuzzer-historical-validation-rc3",
            patch_type="add_column_description",
            deepest_stage_in_attempt=state.deepest_stage_reached,
            outcome="applyability_rejected",
            outcome_reason="dropped_no_op_applier_recycle",
        ))

    real_gate = applier_gate_module.applier_gate
    reverted_gate = ValidationGate(
        name=real_gate.name,
        from_stage=real_gate.from_stage,
        to_stage_on_success=real_gate.to_stage_on_success,
        to_stage_on_reject=FunnelStage.PROPOSED,  # the RC3 revert
        predicate=predicate_always_recycles,
    )
    monkeypatch.setattr(
        applier_gate_module, "applier_gate", reverted_gate,
    )

    artifacts = _run(bundle, tmp_path / "reverted")
    result = check_all_invariants(artifacts)

    # Either A1 fires (QID never reaches terminal) OR the SM emits a
    # different terminal kind without forbidden_signature (B2/E1).
    # Both are valid catches because the regression shape is "no
    # acceptance boundary closed". We assert at least one fires.
    flagged = result.by_id("A1") + result.by_id("B2") + result.by_id("E1")
    assert flagged, (
        f"fuzzer should have flagged the RC3 routing regression "
        f"(applier_gate recycles to PROPOSED); got "
        f"violations={result.violations!r}, "
        f"stage_progress={[(getattr(s, 'qid', ''), str(getattr(getattr(s, 'deepest_stage_reached', None), 'value', ''))) for s in artifacts.final_states]!r}"
    )
