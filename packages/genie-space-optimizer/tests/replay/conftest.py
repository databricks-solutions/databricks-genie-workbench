"""Plan 12 — replay scaffold.

Single-iteration replay harness that drives the Plan 11 Stage 1 → 2 → 3
chain directly (NO harness iteration loop), with per-stage LLM mocking
and a callback that lets each anchor test simulate the downstream
applier / blast-radius / narrow-replacement outcome stream.

Why direct-drive instead of the production iteration loop:

  - The production loop calls cluster_failures → AG selection → lever
    dispatch → applier. Each of those stages owns hundreds of lines of
    branching logic, multiple LLM call sites, and Databricks-backed
    side effects (UC lookups, SQL warehouse execution, MLflow writes).
    A replay test that exercises all of it would need to mock dozens
    of seams and would be fragile to any production refactor.
  - The CONTRACT we're testing is per-intent: every Stage 3
    RepairProposal terminates in exactly one GSO_PATCH_OUTCOME_V1
    marker (I22). The contract doesn't care which lever dispatched
    the proposal or which applier function landed the patch.
  - So the scaffold drives just the Plan 11 stage entry points
    (diagnose_failing_qids → cluster_diagnoses →
    run_plan11_synthesis_for_single_cluster) and lets each test
    supply a post-synthesize callback that emits the recipe-driven
    outcome markers via the canonical emit_patch_outcome.

The deferred PR 3 task ("L6 applier emits APPLIED / VALIDATOR_REJECTED")
is unblocked by this scaffold: tests assert the SHAPE of the outcome
stream (one GSO_PATCH_OUTCOME_V1 per proposal_id with the right
outcome_kind / terminal_reason / collateral_qids / narrow_replacement_*
fields), independent of which production callsite emitted it.
Production wire-in of those emissions is a separate concern.

Public helpers:

  - :func:`run_single_replay_iteration` — the orchestrator
  - :func:`emit_applied_outcome` — convenience wrapper for the
    "Stage 3 → validate → blast-radius → applier → APPLIED" path
  - :func:`emit_blast_radius_with_narrow_replacement` — exercises
    the PR 4 narrow_replacement_from_drop_record wrapper end-to-end
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import (
    AbstainVerdict,
    LlmReasoningResponse,
)


# ── Stage payload helpers ─────────────────────────────────────────────


def _stage_response(
    skill_id: str, parsed: dict | None, declined: AbstainVerdict | None = None,
) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id=f"replay.{skill_id}",
        skill_id=skill_id,
        succeeded=parsed is not None,
        parsed_output=parsed,
        declined=declined,
        raw_text=json.dumps({"result": parsed, "declined": None}),
        tokens_input=100,
        tokens_output=50,
        duration_ms=1,
        error=None,
    )


@dataclass(frozen=True)
class ReplayEmitContext:
    """Context the post-synthesize callback receives so it can emit
    outcome markers without re-discovering the canonical keys.
    """

    optimization_run_id: str
    iteration: int
    ag_id: str
    cluster_id: str


# ── Public emission helpers ───────────────────────────────────────────


def emit_applied_outcome(
    proposal,
    ctx: ReplayEmitContext,
    *,
    applied_patch_id: str | None = None,
) -> None:
    """Simulate the "Stage 3 → validate → blast-radius → applier"
    happy path: emit a single ``GSO_PATCH_OUTCOME_V1`` with
    ``outcome_kind=applied``.

    Stand-in for the deferred PR 3 production wire-in at the L6
    applier callsite. Tests use this to assert the CONTRACT-level
    shape of the outcome stream; production wire-in does the same
    emission but from the live applier hook.
    """
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
    )
    emit_patch_outcome(
        optimization_run_id=ctx.optimization_run_id,
        iteration=ctx.iteration,
        ag_id=ctx.ag_id,
        cluster_id=ctx.cluster_id,
        intent_id=proposal.intent_id,
        outcome_kind=PatchOutcomeKind.APPLIED,
        terminal_reason="",
        applied_patch_id=str(
            applied_patch_id or f"ap_{proposal.intent_id}"
        ),
    )


def emit_blast_radius_with_narrow_replacement(
    proposal,
    ctx: ReplayEmitContext,
    *,
    collateral_qids: tuple[str, ...],
    protected_sql_by_qid: dict[str, str],
    narrowed_patch_body: dict | None,
    cluster,
) -> None:
    """Simulate the PR 4 blast-radius narrow-replacement path
    end-to-end: build a :class:`BlastRadiusDropRecord`, run it through
    :func:`narrow_replacement_from_drop_record` (which dispatches to
    the LLM loop via the patched ``narrow_replacement_with_llm``), and
    emit the resulting ``GSO_PATCH_OUTCOME_V1`` based on whether the
    narrow loop produced a usable replacement.

    Tests patch ``narrow_replacement_with_llm`` to return the narrowed
    proposal (a :class:`RepairProposal`) before calling this helper.
    The helper then emits:

      - ``BLAST_RADIUS_REJECTED`` with ``narrow_replacement_attempted=True``
        and ``narrow_outcome="narrowed"`` when the loop returned a
        proposal, OR
      - ``BLAST_RADIUS_REJECTED`` with ``narrow_replacement_attempted=True``
        and ``narrow_outcome="exhausted"`` when the loop returned None.
    """
    from genie_space_optimizer.optimization.blast_radius_drop_record import (
        BlastRadiusDropRecord,
    )
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
    )
    from genie_space_optimizer.optimization.repair_intent import PatchType
    from genie_space_optimizer.optimization.stages.narrow_replacement import (
        narrow_replacement_from_drop_record,
    )

    patch_type_str = (
        proposal.patch_type.value
        if isinstance(proposal.patch_type, PatchType)
        else str(proposal.patch_type)
    )
    drop_record = BlastRadiusDropRecord(
        intent_id=proposal.intent_id,
        original_patch_type=patch_type_str,
        original_patch_body=dict(proposal.patch_body),
        causal_target=(
            proposal.target_objects[0].columns[0]
            if (proposal.target_objects and proposal.target_objects[0].columns)
            else (
                proposal.blame_set[0] if proposal.blame_set else ""
            )
        ),
        failing_sql_anchor="",
        target_qids=tuple(proposal.target_qids),
        collateral_qids=tuple(collateral_qids),
        protected_sql_by_qid=dict(protected_sql_by_qid),
        rca_card_id="",
        cluster_id=ctx.cluster_id,
        ag_id=ctx.ag_id,
    )
    narrowed = narrow_replacement_from_drop_record(
        drop_record=drop_record,
        cluster=cluster,
        w=None,
        optimization_run_id=ctx.optimization_run_id,
        iteration=ctx.iteration,
    )
    narrow_outcome = "narrowed" if narrowed is not None else "exhausted"
    emit_patch_outcome(
        optimization_run_id=ctx.optimization_run_id,
        iteration=ctx.iteration,
        ag_id=ctx.ag_id,
        cluster_id=ctx.cluster_id,
        intent_id=proposal.intent_id,
        outcome_kind=PatchOutcomeKind.BLAST_RADIUS_REJECTED,
        terminal_reason="blast_radius_rejected",
        collateral_qids=collateral_qids,
        narrow_replacement_attempted=True,
        narrow_outcome=narrow_outcome,
    )


# ── Orchestrator ──────────────────────────────────────────────────────


def run_single_replay_iteration(
    *,
    failing_qids: list[str],
    eval_rows: list[dict],
    diagnose_payload: dict,
    cluster_payload: dict,
    synthesize_payload: dict,
    post_synthesize_outcome_emitter: Callable[
        [list, ReplayEmitContext], None,
    ],
    optimization_run_id: str = "run_replay",
    iteration: int = 1,
    ag_id: str = "AG_REPLAY",
    monkeypatch: pytest.MonkeyPatch | None = None,
) -> str:
    """Drive one replay iteration through Plan 11 Stage 1 → 2 → 3 and
    return the captured stdout.

    The caller passes:

      - ``failing_qids`` / ``eval_rows``: the upstream eval surface.
      - ``diagnose_payload`` / ``cluster_payload`` / ``synthesize_payload``:
        pre-canned LLM responses for each stage. Shape matches each
        skill's ``LLMOutputContract`` (e.g. ``{"diagnoses": [...]}``,
        ``{"clusters": [...]}``, ``{"proposals": [...]}``).
      - ``post_synthesize_outcome_emitter``: callback receiving the
        list of :class:`RepairProposal` Stage 3 produced + a
        :class:`ReplayEmitContext`. The callback is responsible for
        emitting the recipe-driven ``GSO_PATCH_OUTCOME_V1`` markers
        (typically via ``emit_applied_outcome`` or
        ``emit_blast_radius_with_narrow_replacement``).

    ``monkeypatch`` is required so the scaffold can patch the per-stage
    ``LlmReasoningCall``. Tests pass ``request.getfixturevalue("monkeypatch")``
    or accept the fixture in their signature.
    """
    if monkeypatch is None:
        raise ValueError(
            "run_single_replay_iteration requires a monkeypatch fixture "
            "to install the per-stage LLM mocks; pass it explicitly"
        )

    from io import StringIO
    import contextlib

    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
        synthesize as _stage3_mod,
    )
    from genie_space_optimizer.optimization.stages.cluster_plan11 import (
        cluster_diagnoses,
    )
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )

    reset_patch_outcome_emitter()

    # Install per-stage LLM mocks. Each stage uses its own module-level
    # LlmReasoningCall import; patching the bound name on each module
    # ensures the production stage handlers see the mocked instance.
    class _StubLlmReasoningCall:
        def __init__(self, payload: dict | None, skill_id: str):
            self._payload = payload
            self._skill_id = skill_id

        def invoke(self, *, w, request):
            return _stage_response(self._skill_id, self._payload)

    def _make_stub_class(payload: dict | None, skill_id: str):
        class _Stub:
            def __init__(self):
                pass

            def invoke(self, *, w, request):
                return _stage_response(skill_id, payload)

        return _Stub

    monkeypatch.setattr(
        _stage1_mod,
        "LlmReasoningCall",
        _make_stub_class(diagnose_payload, "plan11_diagnose"),
    )
    monkeypatch.setattr(
        _stage2_mod,
        "LlmReasoningCall",
        _make_stub_class(cluster_payload, "plan11_cluster"),
    )
    monkeypatch.setattr(
        _stage3_mod,
        "LlmReasoningCall",
        _make_stub_class(synthesize_payload, "plan11_synthesize"),
    )

    captured = StringIO()
    with contextlib.redirect_stdout(captured):
        # Stage 1.
        diagnoses = diagnose_failing_qids(
            failing_qids=[
                _qid_input_for(qid, eval_rows) for qid in failing_qids
            ],
            schema_columns=[],
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            w=None,
        )

        # Stage 2.
        clusters = cluster_diagnoses(
            diagnoses=diagnoses,
            schema_columns=[],
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            namespace="hard",
            w=None,
        )

        # Stage 3 — one synthesize call per cluster. Collect the
        # surviving proposals (which already had the PR 2 survival
        # contract applied + the PR 7 stage marker emitted) so the
        # post-synthesize callback can drive outcome emission.
        from genie_space_optimizer.optimization.repair_proposal_typed import (
            RepairProposal,
        )
        all_proposals: list[RepairProposal] = []
        for cluster in clusters:
            result = run_plan11_synthesis_for_single_cluster(
                cluster=cluster,
                schema_slice={},
                history=[],
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                ag_id=ag_id,
                w=None,
            )
            if result.proposal is None:
                continue
            # ClusterSynthesisResult.proposal is the legacy dict shape
            # (the first RepairProposal projected to dict). Round-trip
            # back to RepairProposal so the callback gets a typed
            # surface.
            try:
                p = RepairProposal.from_json(result.proposal)
                all_proposals.append(p)
            except Exception:  # noqa: BLE001
                # Defensive: if the projection ever produces a
                # malformed dict, surface the failure as an empty
                # proposal list rather than crashing the replay.
                pass

        # Hand to the caller's outcome emitter, one cluster context per
        # cluster. (Today we collapse to one context because Stage 3
        # carries a single proposal per cluster; per-proposal contexts
        # would mirror the production callsite stream.)
        cluster_id = clusters[0].cluster_id if clusters else ""
        ctx = ReplayEmitContext(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            ag_id=ag_id,
            cluster_id=cluster_id,
        )
        post_synthesize_outcome_emitter(all_proposals, ctx)

    return captured.getvalue()


def _qid_input_for(qid: str, eval_rows: list[dict]) -> dict:
    """Look up the eval_row for a QID and project into the
    ``failing_qids`` shape ``diagnose_failing_qids`` expects."""
    for row in eval_rows:
        if str(row.get("question_id")) == qid:
            return {
                "qid": qid,
                "question_text": str(row.get("question") or ""),
                "ground_truth_sql": str(row.get("ground_truth_sql") or ""),
                "generated_sql": str(row.get("generated_sql") or ""),
                "judge_rationale": str(row.get("judge_rationale") or ""),
                "blame_set_seed": list(row.get("blame_set_seed") or []),
            }
    return {
        "qid": qid,
        "question_text": "",
        "ground_truth_sql": "",
        "generated_sql": "",
        "judge_rationale": "",
        "blame_set_seed": [],
    }


def parse_patch_outcome_markers(stdout: str) -> list[dict]:
    """Pull every ``GSO_PATCH_OUTCOME_V1`` marker out of captured
    stdout and return the parsed payloads. Tests use this to assert
    on the outcome stream shape without re-parsing the marker format
    each time.
    """
    out: list[dict] = []
    for line in stdout.splitlines():
        if line.startswith("GSO_PATCH_OUTCOME_V1 "):
            try:
                out.append(json.loads(line.partition(" ")[2]))
            except json.JSONDecodeError:
                continue
    return out
