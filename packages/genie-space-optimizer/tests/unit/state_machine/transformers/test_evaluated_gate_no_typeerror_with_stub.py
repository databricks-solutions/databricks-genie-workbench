"""Trial 15 — drive ``evaluated_gate`` through APPLIED -> EVALUATED via
the ``extras['post_apply_eval']`` stub and assert no ``TypeError``.

Why this test exists:
    Production postmortems dc89d1a9 and 98ec8950 showed every applied
    patch dying with ``post_apply_eval_failed: run_evaluation()
    argument after ** must be a mapping, not NoneType``. The root
    cause was ``ctx.eval_kwargs is None`` propagating into
    ``evaluate_post_patch``. Trial 15 closes that with two seams:

      1. ``run_state_machine_iteration_and_persist`` plumbs the real
         ``eval_kwargs`` from the harness (covered by D1).
      2. ``evaluated_gate._run_post_apply_eval`` recognizes an
         ``extras['post_apply_eval']`` stub for workbench / unit-test
         paths that have no live MLflow + Genie backend.

    This test exercises seam (2): the gate must produce a clean
    ``accepted`` / ``rejected`` verdict via the stub and the
    ``GSO_GATE_REASONING_V1`` marker it emits must NOT carry
    ``exception_type: TypeError`` in its predicate inputs.
"""
from __future__ import annotations

import io
from contextlib import redirect_stdout

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    AppliedRecord,
    ClusterMembershipRecord,
    DiagnosisRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers.evaluated_gate import (
    evaluated_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_applied(qid: str = "gs_trial15"):
    """Hand-roll a state already at APPLIED so the gate can act on it."""
    s = build_initial_state(
        qid=qid,
        iteration=1,
        seen=HardQidSeenRecord(
            "row-1", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1,
        ),
    )
    transitions = (
        (
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED,
            {
                "diagnosed": DiagnosisRecord(
                    "plan11_stage1", "k", "s", "f", "e", "high", "r",
                ),
            },
        ),
        (
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED,
            {
                "clustered": ClusterMembershipRecord(
                    "H1", "AG", (qid,), 6, "k",
                ),
            },
        ),
        (
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED,
            {
                "proposals": (
                    ProposalAttempt(
                        0, "intent_i", "add_column_description",
                        FunnelStage.APPLIED, "applied", "ok",
                    ),
                ),
            },
        ),
        (FunnelStage.PROPOSED, FunnelStage.NORMALIZED, {}),
        (FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, {}),
        (
            FunnelStage.APPLYABLE, FunnelStage.APPLIED,
            {"applied": AppliedRecord(1, "call_abc", 0, ("intent_i",))},
        ),
    )
    for from_s, to_s, kw in transitions:
        s = s.advance(
            to_s,
            StageTransition(from_s, to_s, 1, "t", "validation_gate"),
            **kw,
        )
    return s


def test_evaluated_gate_uses_post_apply_eval_stub_without_typeerror():
    """The stub returns ``(score, sql, eval_row_id)`` and the gate must
    produce an EVALUATED state — no TypeError, no
    OPTIMIZER_INVARIANT_VIOLATION terminal."""
    state = _state_at_applied()

    def _stub(*, state, ctx):
        return (0.9, "SELECT 2", "row_post_1")

    ctx = TransformerContext(
        iteration=1,
        run_id="trial15-d2",
        validation_context=ValidationContext(1, "trial15-d2", {}),
        extras={"post_apply_eval": _stub},
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        result = evaluated_gate.transform(state, ctx)
    stdout = buf.getvalue()

    assert result.current_stage == FunnelStage.EVALUATED, (
        f"Expected EVALUATED after stub returned a real tuple; got "
        f"{result.current_stage}. terminal={result.terminal!r}"
    )
    assert result.evaluated is not None
    assert result.evaluated.pre_apply_score == 0.0
    assert result.evaluated.post_apply_score == 0.9
    assert result.evaluated.post_apply_sql == "SELECT 2"

    # The gate must NOT have emitted the TypeError predicate-input
    # signature that the postmortems flagged.
    assert "post_apply_eval_failed" not in stdout, (
        f"Gate emitted post_apply_eval_failed marker despite stub being "
        f"provided. The stub path is broken or unreachable. Stdout:\n{stdout}"
    )
    assert "TypeError" not in stdout, (
        f"Gate stdout contains TypeError — the stub should have prevented "
        f"any call into evaluate_post_patch. Stdout:\n{stdout}"
    )


def test_evaluated_gate_without_stub_or_eval_kwargs_terminates_cleanly():
    """When no stub and no ``eval_kwargs`` are wired and the gate falls
    through to ``evaluate_post_patch(**None)``, the gate's broad except
    must catch the TypeError and emit a ``post_apply_eval_failed``
    terminal — this matches the dc89/98ec postmortem signature so
    operators can grep for it.

    This is the "before Trial 15" failure mode preserved as a
    regression marker. The Trial 15 fix at the SM-lane seam
    (optimizer.py invariant) prevents production from ever reaching
    this gate without one of the two paths wired; this test pins the
    gate's own defensive behavior in case a future regression
    bypasses the seam.
    """
    state = _state_at_applied()
    ctx = TransformerContext(
        iteration=1,
        run_id="trial15-d2-fallback",
        validation_context=ValidationContext(1, "trial15-d2-fallback", {}),
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        result = evaluated_gate.transform(state, ctx)
    stdout = buf.getvalue()

    # The gate's defensive except still produces a typed terminal
    # rather than letting the TypeError escape upward; that contract
    # is what kept earlier trials' postmortems readable.
    assert result.terminal is not None
    assert "post_apply_eval_failed" in str(result.terminal.reason)
