"""Local state-machine runner for the workbench.

Builds a ``TransformerContext`` directly so the workbench can:

* keep ``ctx.w`` as a real Databricks ``WorkspaceClient`` for LLM mode,
* install a recording applier stub in ``ctx.extras["applier"]`` so the
  applier gate never touches a Genie space,
* swap in a reduced ``stage1-only`` registry that stops after Stage 1
  when the operator only wants to test hydration + request envelope
  + Stage 1 schema parsing.

The runner reuses the production state machine orchestrator unchanged;
the only workbench-specific bits are the registry choice and the
recording applier. The production transformers themselves are exactly
what would run in a deploy, which is the point — workbench surprises
are production surprises caught early.
"""
from __future__ import annotations

import io
import logging
import os
import time
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
from typing import Any, Iterator

logger = logging.getLogger(__name__)

from local_lever_workbench.models import (
    StageProgress,
    WorkbenchInputBundle,
    WorkbenchRunConfig,
)
from local_lever_workbench.recording_applier import (
    PatchRecorder,
    make_recording_applier_stub,
)


# ── LLM mode constants ────────────────────────────────────────────────

LLM_MODE_LIVE = "live-databricks"
LLM_MODE_TAPE = "sm-tape"
LLM_MODE_STAGE1_ONLY = "stage1-only"
# Trial 16 v1.8 — live-llm-only: live Stage 1/2/3 LLM endpoints but
# stubbed post-apply eval / applier. Catches strategist+lever prompt
# regressions without needing Spark, Genie API access, or live MLflow
# scorers. The full V1.5 registry runs end-to-end; only the LLM hops
# and gates execute against real services. Acceptance/evaluated gates
# operate on ``bundle.post_apply_eval_tape`` exactly like sm-tape mode.
LLM_MODE_LIVE_LLM_ONLY = "live-llm-only"
SUPPORTED_LLM_MODES = (
    LLM_MODE_LIVE,
    LLM_MODE_LIVE_LLM_ONLY,
    LLM_MODE_TAPE,
    LLM_MODE_STAGE1_ONLY,
)


def _is_live_llm(llm_mode: str) -> bool:
    """True for any mode that issues real LLM-endpoint calls."""
    return llm_mode in (LLM_MODE_LIVE, LLM_MODE_LIVE_LLM_ONLY)


def _needs_canary_stack(llm_mode: str) -> bool:
    """True for modes that need Spark + predict_fn + scorers + Genie API.

    Only full ``live-databricks`` exercises the post-apply eval against
    a real warehouse. ``live-llm-only`` deliberately keeps the canary
    stack stubbed so the operator can iterate on prompt regressions
    without a serverless cluster.
    """
    return llm_mode == LLM_MODE_LIVE

APPLY_MODE_FAKE_RECORD = "fake-record"
SUPPORTED_APPLY_MODES = (APPLY_MODE_FAKE_RECORD,)


# ── Result wrapper ───────────────────────────────────────────────────


@dataclass(frozen=True)
class LocalRunArtifacts:
    """Raw artefacts produced by one workbench SM run.

    The funnel report builder consumes these to produce the operator-
    facing JSON and Markdown outputs.
    """

    final_states: tuple[Any, ...]  # tuple[QuestionStateInIteration, ...]
    stdout_text: str
    recorder: PatchRecorder
    elapsed_seconds: float


# ── Registries ───────────────────────────────────────────────────────


def _build_registry(llm_mode: str):
    """Return the FunnelStage→transformers mapping for ``llm_mode``.

    * ``stage1-only`` runs Stage 1 alone — the cheapest live signal
      that hydration + request envelope + Stage 1 schema parsing all
      work end-to-end (HARD_QID_SEEN → DIAGNOSED only).
    * ``live-databricks`` and ``sm-tape`` run the full V1.5 pipeline
      (HARD_QID_SEEN → ACCEPTED via Trial 15 plumbing). Registry includes
      ``evaluated_gate`` at APPLIED and ``acceptance_gate`` at EVALUATED
      for both modes. ``live-databricks`` uses real ``eval_kwargs`` /
      ``stage_ctx``; ``sm-tape`` keeps the Trial 15 ``post_apply_eval``
      stub in ``extras``.
    * ``live-llm-only`` (v1.8) runs the same full V1.5 pipeline but
      sources Stage 1/2/3 outputs from real LLM endpoints while the
      post-apply eval falls back to the same tape stub used by
      ``sm-tape``. The mode is the cheapest way to surface strategist
      / lever prompt regressions before they reach a real job run.
    """
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate import (
        acceptance_gate,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.applier_gate import (
        applier_gate,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.blast_radius_batch import (
        blast_radius_batch,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.cluster_batch import (
        plan11_stage2_clustering,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
        plan11_stage1_diagnosis,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.evaluated_gate import (
        evaluated_gate,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.narrow_replacement_gate import (
        narrow_replacement_gate,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.structural_repair_gate import (
        structural_repair_gate,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.synthesize_llm import (
        plan11_stage3_synthesis,
    )

    if llm_mode == LLM_MODE_STAGE1_ONLY:
        return {FunnelStage.HARD_QID_SEEN: (plan11_stage1_diagnosis,)}

    return {
        FunnelStage.HARD_QID_SEEN: (plan11_stage1_diagnosis,),
        FunnelStage.DIAGNOSED:     (plan11_stage2_clustering,),
        FunnelStage.CLUSTERED:     (plan11_stage3_synthesis,),
        FunnelStage.PROPOSED:      (structural_repair_gate,),
        FunnelStage.NORMALIZED:    (blast_radius_batch, narrow_replacement_gate),
        FunnelStage.APPLYABLE:     (applier_gate,),
        FunnelStage.APPLIED:       (evaluated_gate,),
        FunnelStage.EVALUATED:     (acceptance_gate,),
    }


# ── Workspace client construction ────────────────────────────────────


def _build_live_workspace_client(profile: str | None):
    """Construct a real Databricks ``WorkspaceClient`` for live LLM mode.

    Authentication discovery order matches the standard Databricks SDK
    pattern: an explicit ``profile`` argument wins over environment
    variables. The workbench fails fast with a clear error if neither
    is configured, because falling back to silent unauthenticated
    behaviour would defeat the purpose of "the local runner exercises
    the real envelope path".
    """
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "live-databricks mode requires databricks-sdk to be importable. "
            "Run from the package venv (uv sync --frozen) or pass "
            "--llm-mode sm-tape if you only need offline replay."
        ) from exc

    kwargs: dict[str, Any] = {}
    if profile:
        kwargs["profile"] = profile

    ws = WorkspaceClient(**kwargs)

    # Force a credential resolution so a misconfigured profile fails here
    # rather than mid-iteration. ``config.host`` triggers SDK auth setup.
    host = getattr(getattr(ws, "config", None), "host", "")
    if not host:
        raise RuntimeError(
            "WorkspaceClient resolved with no host — set DATABRICKS_HOST or "
            "pass --profile <name> referencing a configured profile in "
            "~/.databrickscfg."
        )
    return ws


def _build_workbench_spark(
    *,
    profile: str | None,
    profile_required: bool = True,
) -> Any | None:
    """Construct a Databricks Connect serverless SparkSession.

    Required for ``live-databricks`` mode because ``make_predict_fn``
    and ``make_all_scorers`` both bind a SparkSession at construction
    time (see evaluation.py:4732 and scorers/__init__.py:106).

    Modes that do NOT need real Spark (``live-llm-only``, ``sm-tape``,
    ``stage1-only``) pass ``profile_required=False`` and accept the
    returned ``None`` — ``live-llm-only`` still drives Stage 1/2/3
    against real LLM endpoints, it only skips the canary eval stack.

    Fail-fast on missing ``databricks-connect`` rather than letting
    the eventual ``make_predict_fn(spark=None, ...)`` raise an
    opaque ``AttributeError`` halfway through evaluated_gate.
    """
    if not profile_required:
        return None

    try:
        from databricks.connect import DatabricksSession  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "live-databricks mode requires the databricks-connect package "
            "for SparkSession construction. Install via "
            "uv add databricks-connect==<dbr-matching-version> or "
            "fall back to --llm-mode sm-tape for offline replay."
        ) from exc

    builder = DatabricksSession.builder.serverless(True)
    if profile:
        builder = builder.profile(profile)
    return builder.getOrCreate()


# ── Tape harness wrapper ─────────────────────────────────────────────


@contextmanager
def _tape_patch_or_noop(
    *, llm_mode: str, tape_path: Any
) -> Iterator[Any | None]:
    """Yield a ``TapeReplayHarness`` when in tape mode, else ``None``.

    Loading the tape lazily keeps the workbench package import-light
    for callers that never use tape mode.
    """
    if llm_mode != LLM_MODE_TAPE:
        yield None
        return
    if tape_path is None:
        raise ValueError(
            "sm-tape mode requires --tape-path pointing at a JSONL tape "
            "file (see tests/integration/sm_tape_replay.py for the "
            "TapeEntry format)."
        )

    # The tape harness lives under tests/integration. The workbench
    # imports it because the harness module itself is import-safe
    # (pure helpers, no test-time side effects), but the workbench's
    # production-facing CLI documents this dependency as dev-only.
    #
    # Two valid import paths depending on entry point:
    #   * pytest discovers ``tests/`` as a rootdir package →
    #     ``tests.integration.sm_tape_replay``
    #   * the CLI prepends ``tests/`` directly to sys.path →
    #     ``integration.sm_tape_replay``
    try:
        from tests.integration.sm_tape_replay import (  # type: ignore[import-not-found]
            TapeReplayHarness,
            load_tape,
        )
    except ModuleNotFoundError:
        from integration.sm_tape_replay import (  # type: ignore[import-not-found]
            TapeReplayHarness,
            load_tape,
        )

    harness = TapeReplayHarness(tape=load_tape(tape_path))
    with harness.patch():
        yield harness


# ── TransformerContext construction ───────────────────────────────────


def _make_ctx_kwargs(
    *,
    llm_mode: str,
    bundle,
    workspace_client,
    spark,
    iteration: int = 1,
    forbidden_signatures: tuple[str, ...] = (),
    baseline_eval_rows: tuple = (),
    post_apply_eval_rows: tuple = (),
    rca_evidence_typed: dict | None = None,
    metadata_snapshot: dict | None = None,
    schema_columns_tuple: tuple[str, ...] = (),
    schema_columns_source: str = "",
    applier_stub=None,
) -> dict:
    """Build the kwargs that become TransformerContext.

    Mode-aware:
    * ``live-databricks`` builds real ``stage_ctx`` + ``eval_kwargs`` so
      evaluated_gate can call the production ``evaluate_post_patch``.
    * ``live-llm-only`` issues real Stage 1/2/3 LLM calls but keeps the
      Trial 15 ``post_apply_eval`` stub — same tape-driven path as
      ``sm-tape``. The strategist/lever prompts execute end-to-end
      against live endpoints; only the canary eval is stubbed.
    * ``sm-tape`` keeps the Trial 15 ``post_apply_eval`` stub in
      ``extras`` so evaluated_gate operates on canned baselines.
    * ``stage1-only`` never reaches evaluated_gate; either path works.
    """
    from genie_space_optimizer.optimization.state_machine.verdict import (
        ValidationContext,
    )

    run_id_local = f"workbench-{int(time.time())}"
    extras: dict[str, Any] = {
        "workspace_client": workspace_client,
        "applier": applier_stub,
        "synthesize_llm": True,
    }
    eval_kwargs = None
    stage_ctx = None
    eval_qids: tuple[str, ...] = ()

    if _needs_canary_stack(llm_mode):
        from genie_space_optimizer.optimization.evaluation import make_predict_fn
        from genie_space_optimizer.optimization.optimizer import (
            _build_canary_stage_ctx_and_eval_kwargs,
        )
        from genie_space_optimizer.optimization.scorers import make_all_scorers

        catalog = str(getattr(bundle, "catalog", "") or "")
        schema = str(getattr(bundle, "schema", "") or "")

        predict_fn = make_predict_fn(
            workspace_client,
            str(bundle.space_id or ""),
            spark,
            catalog,
            schema,
        )
        scorers = make_all_scorers(
            workspace_client,
            spark,
            catalog,
            schema,
        )
        stage_ctx, eval_kwargs = _build_canary_stage_ctx_and_eval_kwargs(
            run_id=run_id_local,
            iteration=int(iteration),
            space_id=str(bundle.space_id or ""),
            domain=str(getattr(bundle, "domain", "") or ""),
            catalog=catalog,
            schema=schema,
            phase_h_anchor_run_id=None,
            w=workspace_client,
            spark=spark,
            exp_name="",
            benchmarks=list(getattr(bundle, "benchmarks", []) or []),
            predict_fn=predict_fn,
            scorers=scorers,
            reference_sqls=getattr(bundle, "reference_sqls", None),
            uc_schema=str(getattr(bundle, "uc_schema", "") or ""),
            max_benchmark_count=len(list(getattr(bundle, "benchmarks", []) or [])),
        )
        eval_qids = tuple(
            str(b.get("question_id") or "")
            for b in (getattr(bundle, "benchmarks", []) or [])
            if b.get("question_id")
        )
    else:
        # Trial 16 v1.6 — drive the stub from
        # ``bundle.post_apply_eval_tape`` when present. The tape's
        # per-qid rows are joined via the canonical
        # ``extract_question_id`` helper so the workbench mirrors
        # production's qid carrier shapes (top-level / slash /
        # dot / nested ``inputs``). Tape miss falls back to the Trial
        # 15 baseline-score stub — preserves the "applier reaches
        # APPLIED, gate aborts at no_post_apply_row_for_qid" scenario
        # for the postmortem-replay tests.
        tape_rows = tuple(getattr(bundle, "post_apply_eval_tape", ()) or ())

        def _workbench_post_apply_eval_stub(*, state, ctx):
            if tape_rows:
                from genie_space_optimizer.optimization._qid_extraction import (
                    extract_question_id,
                )
                for r in tape_rows:
                    if extract_question_id(dict(r))[0] == state.qid:
                        score = float(
                            r.get("feedback/result_correctness/value", 0.0)
                            or 0.0
                        )
                        post_sql = str(r.get("generated_sql") or "")
                        row_id = str(
                            r.get("eval_row_id")
                            or r.get("row_id")
                            or f"workbench:{state.qid}:iter_{ctx.iteration:03d}"
                        )
                        return (score, post_sql, row_id)
            baseline_score = float(getattr(state.seen, "score", 0.0) or 0.0)
            baseline_sql = str(getattr(state.seen, "baseline_sql", "") or "")
            eval_row_id = f"workbench:{state.qid}:iter_{ctx.iteration:03d}"
            return (baseline_score, baseline_sql, eval_row_id)

        extras["post_apply_eval"] = _workbench_post_apply_eval_stub

    return {
        "iteration": int(iteration),
        "run_id": run_id_local,
        "validation_context": ValidationContext(
            int(iteration),
            run_id_local,
            {"workspace_client": workspace_client},
        ),
        "forbidden_signatures": forbidden_signatures,
        "extras": extras,
        "baseline_eval_rows": baseline_eval_rows,
        # Trial 16 v1.6 — populate ``ctx.post_apply_eval_rows`` from
        # ``bundle.post_apply_eval_tape`` so ``acceptance_gate``'s
        # ``_assess_collateral`` can join baseline ↔ post rows and
        # surface collateral regressions. Empty tape = empty post
        # rows; the gate falls back to its target_fixed-only path.
        "post_apply_eval_rows": post_apply_eval_rows,
        "w": workspace_client,
        "space_id": str(bundle.space_id or ""),
        "metadata_snapshot": metadata_snapshot or {},
        "rca_evidence_typed": rca_evidence_typed or {},
        "schema_columns": schema_columns_tuple,
        "schema_columns_source": schema_columns_source,
        "eval_kwargs": eval_kwargs,
        "stage_ctx": stage_ctx,
        "eval_qids": eval_qids,
    }


def _make_ctx_kwargs_for_test(**kwargs):
    return _make_ctx_kwargs(**kwargs)


# ── Entry point ───────────────────────────────────────────────────────


def run_workbench_iteration(
    bundle: WorkbenchInputBundle,
    config: WorkbenchRunConfig,
) -> LocalRunArtifacts:
    """Run one SM iteration locally and return the raw artefacts.

    The function is intentionally side-effect free with respect to the
    bundle: it deep-reads ``bundle.eval_rows`` into a tuple and never
    mutates the input. The only side effects are stdout capture and
    the recording applier's in-memory list of PATCHes.
    """
    if config.llm_mode not in SUPPORTED_LLM_MODES:
        raise ValueError(
            f"Unsupported llm_mode={config.llm_mode!r}. "
            f"Supported: {SUPPORTED_LLM_MODES}."
        )
    if config.apply_mode not in SUPPORTED_APPLY_MODES:
        raise ValueError(
            f"Unsupported apply_mode={config.apply_mode!r}. "
            f"Supported: {SUPPORTED_APPLY_MODES} (V1)."
        )

    # Lazy production imports — pulling these in unconditionally would
    # force every workbench unit test to load the whole optimizer.
    from genie_space_optimizer.optimization.state_machine.orchestrator import (
        StateMachine,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.dispatch_input import (
        build_initial_states_from_eval_rows,
    )
    from genie_space_optimizer.optimization.state_machine.verdict import (
        TransformerContext,
    )

    # Construct workspace client per mode. Both ``live-databricks`` and
    # ``live-llm-only`` need a real WorkspaceClient — the latter so the
    # Stage 1/2/3 transformers can hit the model-serving endpoint.
    workspace_client: Any = None
    if _is_live_llm(config.llm_mode):
        # Honour --llm-model by setting env BEFORE the SDK or the LLM
        # client reads it. The optimizer's llm_client picks LLM_MODEL
        # off os.environ.
        if config.llm_model:
            os.environ["LLM_MODEL"] = config.llm_model
        workspace_client = _build_live_workspace_client(config.profile)

    recorder = PatchRecorder()
    applier_stub = make_recording_applier_stub(recorder)

    eval_rows = bundle.eval_rows
    initial_states = build_initial_states_from_eval_rows(
        eval_rows=eval_rows,
        iteration=config.iteration,
    )
    if not initial_states:
        raise RuntimeError(
            "No hard QIDs admitted from the bundle. The bundle's eval "
            "rows did not pass row_is_hard_failure — workbench has "
            "nothing to optimize. Re-check the bundle or check that "
            "result_correctness/value=='no' and arbiter is not in the "
            "correct-verdict set."
        )

    # Use a baseline-eval-rows snapshot so the diagnose / synthesize
    # transformers can find their per-QID grounding row.
    from genie_space_optimizer.optimization.canonical_eval_row import (
        normalize_eval_row,
    )
    baseline_eval_rows = tuple(normalize_eval_row(r) for r in eval_rows)

    # Trial 13 typed-evidence cutover — rebuild per-QID typed RCA
    # evidence from the bundle and surface it on the SM context. This
    # is the only data-shaping the workbench performs (everything else
    # is production code by reference); without it the SM canonical
    # lane would drop typed evidence at Stage 1 and abort hard QIDs
    # whose rows lack embedded blame/rca — exactly the production bug
    # the workbench is meant to predict. The metadata_snapshot mirror
    # keeps the workbench symmetric with
    # ``run_state_machine_iteration_and_persist``, where
    # ``metadata_snapshot["_rca_evidence_typed"]`` is the established
    # carrier between the harness and the SM.
    from local_lever_workbench.stage1_probe import _rebuild_typed_evidence
    rca_evidence_typed: dict[str, Any] = {}
    for case in bundle.hard_cases:
        if case.typed_evidence is None:
            continue
        ev = _rebuild_typed_evidence(case.typed_evidence)
        if ev is not None:
            rca_evidence_typed[case.qid] = ev

    metadata_snapshot = dict(bundle.metadata_snapshot or {})
    if rca_evidence_typed:
        metadata_snapshot["_rca_evidence_typed"] = dict(rca_evidence_typed)

    # Trial 13l — call the production injector so the workbench exercises
    # the same code path the harness does. In ``live-databricks`` mode
    # this re-fetches ``serialized_space`` from the Genie API and
    # overwrites ``metadata_snapshot["schema_columns"]`` with the live
    # FQNs (Priority 1). In ``stage1-only`` / ``tape`` mode the
    # workspace_client is ``None``, the injector cleanly returns
    # ``source="api_error"``, and any pre-existing ``schema_columns``
    # from the bundle loader (Trial 13j v2 capture) is retained.
    # Marker emission via ``logger.info`` so postmortems and the
    # tape-mode regression sweep can both grep on it.
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_schema_columns_injection_marker as _schema_cols_marker,
    )
    from genie_space_optimizer.optimization.schema_columns import (
        _derive_schema_columns,
        inject_schema_columns_into_metadata_snapshot as _inject_schema_cols,
    )
    try:
        _inj_ok, _inj_src, _inj_cnt, _inj_lat = _inject_schema_cols(
            metadata_snapshot,
            genie_space_id=str(bundle.space_id or ""),
            client=workspace_client,
        )
        print(
            _schema_cols_marker(
                optimization_run_id=f"workbench-{int(time.time())}",
                iteration=int(config.iteration),
                space_id=str(bundle.space_id or ""),
                injected=bool(_inj_ok),
                source=str(_inj_src),
                column_count=int(_inj_cnt),
                latency_ms=int(_inj_lat),
            )
        )
    except Exception:
        logger.debug(
            "Trial 13l: workbench schema_columns injection failed (non-fatal)",
            exc_info=True,
        )

    # Trial 13i — populate ``ctx.schema_columns`` from the same fan-in
    # the production SM lane uses (metadata_snapshot -> typed evidence
    # union -> identifier allowlist). Without this, every Stage 1 LLM
    # call on workbench bundles received ``"schema_columns": []`` and
    # capture-only QIDs declined with ``insufficient_blame_set``
    # regardless of seed quality. Mirrors
    # ``run_state_machine_iteration_and_persist``.
    schema_columns_tuple, schema_columns_source = _derive_schema_columns(
        metadata_snapshot=metadata_snapshot,
        rca_evidence_typed=rca_evidence_typed or None,
        uc_columns=metadata_snapshot.get("uc_columns"),
    )

    spark = _build_workbench_spark(
        profile=getattr(config, "profile", None),
        profile_required=_needs_canary_stack(config.llm_mode),
    )

    # Trial 16 v1.6 — pass the tape through to acceptance_gate via
    # ``ctx.post_apply_eval_rows``. Normalize each row so the canonical
    # qid extractor + score reader treat them identically to MLflow
    # rows (the eval_rows path the production gate sees).
    post_apply_eval_rows_tuple = tuple(
        normalize_eval_row(dict(r))
        for r in (getattr(bundle, "post_apply_eval_tape", ()) or ())
    )

    ctx_kwargs = _make_ctx_kwargs(
        llm_mode=config.llm_mode,
        bundle=bundle,
        workspace_client=workspace_client,
        spark=spark,
        iteration=config.iteration,
        forbidden_signatures=(),
        baseline_eval_rows=baseline_eval_rows,
        post_apply_eval_rows=post_apply_eval_rows_tuple,
        rca_evidence_typed=rca_evidence_typed,
        metadata_snapshot=metadata_snapshot,
        schema_columns_tuple=schema_columns_tuple,
        schema_columns_source=schema_columns_source,
        applier_stub=applier_stub,
    )
    ctx = TransformerContext(**ctx_kwargs)

    sm = StateMachine(transformers=_build_registry(config.llm_mode))

    buf = io.StringIO()
    t0 = time.monotonic()
    with redirect_stdout(buf), _tape_patch_or_noop(
        llm_mode=config.llm_mode,
        tape_path=config.tape_path,
    ):
        final_states = sm.run_iteration(initial_states, ctx)
    elapsed = time.monotonic() - t0

    return LocalRunArtifacts(
        final_states=tuple(final_states),
        stdout_text=buf.getvalue(),
        recorder=recorder,
        elapsed_seconds=elapsed,
    )


def summarize_stage_progress(
    artifacts: LocalRunArtifacts,
) -> tuple[StageProgress, ...]:
    """Project the final SM states into the workbench's funnel summary."""
    out: list[StageProgress] = []
    for s in artifacts.final_states:
        terminal_reason = ""
        terminal_message = ""
        if s.terminal is not None:
            terminal_reason = str(getattr(s.terminal, "reason", "") or "")
            terminal_message = str(getattr(s.terminal, "message", "") or "")
        deepest = getattr(s.deepest_stage_reached, "value", "")
        out.append(
            StageProgress(
                qid=str(s.qid),
                deepest_stage=str(deepest),
                terminal_reason=terminal_reason,
                terminal_message=terminal_message,
            )
        )
    return tuple(out)


__all__ = [
    "APPLY_MODE_FAKE_RECORD",
    "LLM_MODE_LIVE",
    "LLM_MODE_LIVE_LLM_ONLY",
    "LLM_MODE_STAGE1_ONLY",
    "LLM_MODE_TAPE",
    "LocalRunArtifacts",
    "SUPPORTED_APPLY_MODES",
    "SUPPORTED_LLM_MODES",
    "run_workbench_iteration",
    "summarize_stage_progress",
]
