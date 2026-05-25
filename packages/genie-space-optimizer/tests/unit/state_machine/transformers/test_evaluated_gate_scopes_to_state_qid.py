"""Trial 16.2 — ``evaluated_gate._run_post_apply_eval`` must scope the
per-applied-patch evaluation to ``state.qid`` ONLY, regardless of how many
qids are in ``ctx.eval_qids``.

Why this test exists:
    Trial 15 plumbed ``ctx.eval_qids`` through the gate with
    ``eval_qids=tuple(ctx.eval_qids) or (state.qid,)``. While
    ``ctx.eval_qids`` was always empty (Trial 16.1 helper bug), the
    fallback to ``(state.qid,)`` made the gate's scoping correct by
    accident. Trial 16.1 fixed the helper so ``ctx.eval_qids`` now
    correctly carries the full iteration's qid context (e.g. all 23
    benchmark qids). That exposed the latent bug: the gate now passes
    all 23 qids to ``_run_full_evaluation``, which slices to all 23
    benchmarks, which runs a full 12-minute evaluation per applied
    patch — the exact failure pattern Trial 16 RC1 was supposed to
    prevent (postmortems 575892594490176 + 319530250904653 timed out
    after ~120 min from repeated full evals).

    The conceptual error: ``ctx.eval_qids`` means "qids in scope for
    THIS iteration"; the gate's per-applied-patch eval needs "the qid
    we just patched". They are different concerns conflated since
    Trial 15.

The fix is local: the gate always scopes to ``(state.qid,)``. The
``ctx.eval_qids`` field is still useful for OTHER consumers (the
once-per-iteration baseline at harness.py:_eval_inp_full, classification
metadata in gate_reasoning_marker), but NOT for this per-state slice.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
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


def _state_at_applied(qid: str = "gs_026"):
    """Build a QuestionStateInIteration at FunnelStage.APPLIED for ``qid``."""
    s = build_initial_state(
        qid=qid,
        iteration=1,
        seen=HardQidSeenRecord(
            "row_baseline", "row_is_hard_failure", 0.0,
            "SELECT BASELINE", "x", 1,
        ),
    )
    for from_s, to_s, kw in (
        (
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED,
            {
                "diagnosed": DiagnosisRecord(
                    "plan11_stage1", "k", "s", "f", "e", "high", "r",
                )
            },
        ),
        (
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED,
            {"clustered": ClusterMembershipRecord("H1", "AG", (qid,), 6, "k")},
        ),
        (
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED,
            {
                "proposals": (
                    ProposalAttempt(
                        0, "i", "p", FunnelStage.APPLIED, "applied", "ok",
                    ),
                )
            },
        ),
        (FunnelStage.PROPOSED, FunnelStage.NORMALIZED, {}),
        (FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, {}),
        (
            FunnelStage.APPLYABLE, FunnelStage.APPLIED,
            {"applied": AppliedRecord(1, "call_abc", 0, ("i",))},
        ),
    ):
        s = s.advance(
            to_s,
            StageTransition(from_s, to_s, 1, "t", "validation_gate"),
            **kw,
        )
    return s


def _fake_post_apply_row(qid: str) -> dict:
    """Synthetic patched-state eval row matching ``qid`` in canonical form."""
    return {
        "question_id": qid,
        "feedback/result_correctness/value": 1.0,
        "generated_sql": "SELECT POSTAPPLY",
        "eval_row_id": "row_post_evaluated",
    }


def test_gate_scopes_to_state_qid_only_when_ctx_has_multiple_qids():
    """The acceptance-boundary regression test.

    Production reality: ``ctx.eval_qids`` carries the full
    benchmark-qid context for the iteration (post-Trial-16.1 fix:
    typically 23 qids for the airline domain). The gate is invoked
    per-applied-patch — each patch targets exactly one qid
    (``state.qid``).

    Pre-Trial-16.2 behaviour: the gate forwarded ``ctx.eval_qids`` as
    the eval scope, causing ``_run_full_evaluation`` to slice to all
    23 benchmarks and trigger the full 12-minute eval per applied
    patch.

    Post-Trial-16.2 behaviour: the gate ALWAYS scopes to
    ``(state.qid,)``, so the slice picks one benchmark, ``run_evaluation``
    runs a single-question eval (~30 s instead of ~12 min), and the
    iteration converges in seconds rather than hours.
    """
    state = _state_at_applied(qid="gs_026")

    # Ctx mirrors what Trial 16.1's harness fix populates: the full
    # benchmark-qid context for this iteration.
    ctx = TransformerContext(
        iteration=1,
        run_id="trial16-rc2b",
        validation_context=ValidationContext(1, "trial16-rc2b", {}),
        extras={},
        eval_qids=("gs_001", "gs_009", "gs_026"),
        eval_kwargs={"benchmarks": []},
    )

    captured: dict[str, object] = {}

    class _FakeResult:
        eval_rows = (_fake_post_apply_row("gs_026"),)

    def _spy_evaluate_post_patch(stage_ctx, inp, *, eval_kwargs=None):
        captured["eval_qids"] = tuple(inp.eval_qids)
        return _FakeResult()

    with patch(
        "genie_space_optimizer.optimization.stages.evaluation."
        "evaluate_post_patch",
        side_effect=_spy_evaluate_post_patch,
    ):
        result = evaluated_gate.transform(state, ctx)

    # Hard contract: the per-applied-patch eval is scoped to one qid
    # regardless of how many qids are in flight for the iteration.
    assert captured["eval_qids"] == ("gs_026",), (
        "evaluated_gate must scope the per-applied-patch eval to "
        f"(state.qid,) only — got {captured['eval_qids']!r}. Forwarding "
        "ctx.eval_qids re-introduces the Trial 16 RC1 timeout pattern "
        "(every applied patch triggers a full 23-question post-apply "
        "evaluation; ~12 min per patch; 7 patches -> ~84 min -> 120 min "
        "timeout, recorded in postmortems 575892594490176 and "
        "319530250904653)."
    )

    assert result.current_stage == FunnelStage.EVALUATED
    assert result.evaluated is not None
    assert result.evaluated.post_apply_score == 1.0


def test_gate_scopes_to_state_qid_only_when_ctx_eval_qids_empty():
    """Empty ``ctx.eval_qids`` must still scope to ``(state.qid,)``.

    The pre-Trial-16.2 fallback ``or (state.qid,)`` happened to produce
    the right shape in this case. After Trial 16.2 the gate always
    passes ``(state.qid,)``, so this case still passes — but with a
    cleaner, intention-revealing call.
    """
    state = _state_at_applied(qid="gs_009")

    ctx = TransformerContext(
        iteration=1,
        run_id="trial16-rc2b",
        validation_context=ValidationContext(1, "trial16-rc2b", {}),
        extras={},
        eval_qids=(),
        eval_kwargs={"benchmarks": []},
    )

    captured: dict[str, object] = {}

    class _FakeResult:
        eval_rows = (_fake_post_apply_row("gs_009"),)

    def _spy_evaluate_post_patch(stage_ctx, inp, *, eval_kwargs=None):
        captured["eval_qids"] = tuple(inp.eval_qids)
        return _FakeResult()

    with patch(
        "genie_space_optimizer.optimization.stages.evaluation."
        "evaluate_post_patch",
        side_effect=_spy_evaluate_post_patch,
    ):
        result = evaluated_gate.transform(state, ctx)

    assert captured["eval_qids"] == ("gs_009",)
    assert result.current_stage == FunnelStage.EVALUATED


def test_gate_scopes_to_namespaced_state_qid_even_when_ctx_has_bare_qids():
    """The state.qid → benchmark.qid contract is strict equality.

    Production reality: ``state.qid`` is the canonical namespaced form
    (e.g. ``airline_ticketing_and_fare_analysis_gs_009``); benchmark
    rows carry the same namespaced canonical qid. Strict equality wins.

    Hypothetical mismatch case (analyst's review): if benchmarks ever
    carried only the bare ``gs_009`` form while state used the namespaced
    form, the slice would be empty and ``_run_full_evaluation`` raises
    ``PostApplyEvalEmptySliceError`` — the gate converts that into a
    typed terminal with ``forbidden_signature``, so the Strategist can
    learn and adjust on the next iteration.

    Either way, the slice is scoped to ``state.qid`` ONLY — never the
    wider ``ctx.eval_qids`` set. This test guards that contract.
    """
    namespaced_qid = "airline_ticketing_and_fare_analysis_gs_009"
    state = _state_at_applied(qid=namespaced_qid)

    # ctx carries multiple namespaced qids — analogous to the airline
    # domain at runtime (23 benchmark qids, all namespaced).
    ctx = TransformerContext(
        iteration=1,
        run_id="trial16-rc2b",
        validation_context=ValidationContext(1, "trial16-rc2b", {}),
        extras={},
        eval_qids=(
            "airline_ticketing_and_fare_analysis_gs_026",
            "airline_ticketing_and_fare_analysis_gs_001",
            namespaced_qid,
        ),
        eval_kwargs={"benchmarks": []},
    )

    captured: dict[str, object] = {}

    class _FakeResult:
        eval_rows = (_fake_post_apply_row(namespaced_qid),)

    def _spy_evaluate_post_patch(stage_ctx, inp, *, eval_kwargs=None):
        captured["eval_qids"] = tuple(inp.eval_qids)
        return _FakeResult()

    with patch(
        "genie_space_optimizer.optimization.stages.evaluation."
        "evaluate_post_patch",
        side_effect=_spy_evaluate_post_patch,
    ):
        result = evaluated_gate.transform(state, ctx)

    # The slice is scoped to state.qid (the namespaced canonical
    # form) — not all 3 namespaced ctx qids.
    assert captured["eval_qids"] == (namespaced_qid,), (
        "evaluated_gate must scope to state.qid even when ctx.eval_qids "
        f"carries the full namespaced context — got {captured['eval_qids']!r}."
    )

    assert result.current_stage == FunnelStage.EVALUATED
    assert result.evaluated is not None
    assert result.evaluated.post_apply_score == 1.0


def test_namespace_base_qid_mismatch_produces_typed_terminal_via_empty_slice():
    """Analyst's second concern: namespace/base-qid mismatch.

    If ``state.qid`` is the namespaced canonical form and the benchmark
    row carries only the bare ``gs_009`` form (a producer-side
    misroute), strict equality in ``_run_full_evaluation`` produces an
    empty slice → ``PostApplyEvalEmptySliceError`` → gate maps that to
    a typed terminal with the failure encoded in ``forbidden_signature``.

    This is the intended behaviour: surface the producer-side mismatch
    loudly via a typed terminal so the Strategist can learn from it on
    the next iteration. Magic suffix-matching would paper over the
    producer-side bug and violate "LLM for reasoning, code for
    validation".
    """
    namespaced_qid = "airline_ticketing_and_fare_analysis_gs_009"
    state = _state_at_applied(qid=namespaced_qid)

    ctx = TransformerContext(
        iteration=1,
        run_id="trial16-rc2b",
        validation_context=ValidationContext(1, "trial16-rc2b", {}),
        extras={},
        eval_qids=(namespaced_qid,),
        # Benchmark row uses the bare form — producer-side misroute.
        eval_kwargs={"benchmarks": [{"question_id": "gs_009"}]},
    )

    # No patches needed: ``_run_full_evaluation`` slices benchmarks
    # against ``inp.eval_qids`` BEFORE invoking ``run_evaluation``. The
    # empty slice short-circuits with ``PostApplyEvalEmptySliceError``,
    # which the gate catches and converts to a typed terminal — so the
    # network/MLflow eval primitives are never reached.
    result = evaluated_gate.transform(state, ctx)

    # Gate maps any post-apply eval failure to a typed terminal —
    # acceptance boundary holds, optimizer can keep iterating with
    # learned ``forbidden_signature`` feedback.
    assert result.terminal is not None, (
        f"expected typed terminal but got {result.current_stage!r}: "
        f"{result.evaluated!r}"
    )
    # The terminal's forbidden_signature must reflect the empty-slice
    # cause so the Strategist sees actionable feedback.
    sig = getattr(result.terminal, "forbidden_signature", "") or ""
    assert "post_apply_eval" in sig.lower() or "empty_slice" in sig.lower() or "no_post_apply_row" in sig.lower(), (
        f"terminal.forbidden_signature does not carry the namespace "
        f"mismatch cause — got {sig!r}"
    )


if __name__ == "__main__":
    # Allow direct invocation for fast iteration.
    raise SystemExit(pytest.main([__file__, "-v"]))
