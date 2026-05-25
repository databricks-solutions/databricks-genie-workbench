"""
Metadata Optimizer — failure analysis, proposal generation, conflict detection.

Analyzes evaluation failures (ASI) + current metadata snapshot to produce
targeted metadata change proposals grouped by lever.  LLM calls use
Databricks Claude Opus 4.6 via the Foundation Model API.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from databricks.sdk import WorkspaceClient
from genie_space_optimizer._workspace_client import make_workspace_client
from genie_space_optimizer.optimization.databricks_request_contract import (
    DEFAULT_CONTRACT as _DATABRICKS_REQUEST_CONTRACT,
    RequestEnvelopeInvalidError,
)
from genie_space_optimizer.optimization.llm_client import (
    _openai_client_cache,
    _resolve_bearer_token,
    call_llm as _call_llm_openai,
    get_openai_client as _get_openai_client,
)
from genie_space_optimizer.optimization.rca_execution import (
    clusters_share_defect_identity,
)


def _build_state_machine_initial_states(
    *, eval_rows, iteration: int,
):
    """Plan v3 PR 1.1 — wrap build_initial_states_from_eval_rows for the
    iteration loop callsite.

    Kept as a thin wrapper so the optimizer module surface owns the
    call site that the dispatch-decision marker (legacy) reports on.
    Phase 5 will delete the legacy marker writer; the state machine's
    witness markers replace it.

    Lazy import: ``state_machine.transformers.dispatch_input`` pulls
    from ``optimization.evaluation`` which transitively imports back
    into ``optimization.optimizer`` (via ``stages.clustering``). A
    top-level import would create a circular reference at module-load
    time. The lazy import resolves at call time, after all modules
    have finished loading.
    """
    from genie_space_optimizer.optimization.state_machine.transformers.dispatch_input import (
        build_initial_states_from_eval_rows,
    )
    return build_initial_states_from_eval_rows(eval_rows, iteration=iteration)


def run_state_machine_iteration_and_persist(
    *,
    eval_rows,
    iteration: int,
    run_id: str,
    run_root,
    workspace_client=None,
    forbidden_signatures: tuple[str, ...] = (),
    space_id: str = "",
    metadata_snapshot=None,
    # Trial 15 — SM evaluator boundary contract. The legacy harness lane
    # constructs StageContext + RunEvaluationKwargs at harness.py:16270-
    # 16306 for ``_run_gate_checks`` but never plumbed them into the SM
    # lane; ``evaluated_gate._run_post_apply_eval`` then called
    # ``run_evaluation(**None)`` and every applied patch died with
    # ``OPTIMIZER_INVARIANT_VIOLATION post_apply_eval_failed:TypeError``.
    # Threading these four fields closes the boundary. All default-None
    # so pre-Trial-15 callsites (tests, devtools) stay byte-stable.
    stage_ctx=None,
    eval_kwargs=None,
    eval_qids: tuple[str, ...] = (),
    baseline_eval=None,
    # Trial 15 — caller-supplied extras get merged into the SM
    # ``TransformerContext.extras`` dict. Production callers do not use
    # this; it exists so the workbench escape hatch
    # (``extras['post_apply_eval'] = <stub>``) and the structural
    # invariant test in D1 can exercise the function without needing a
    # live evaluation backend.
    extras: dict | None = None,
):
    """Plan v3 PR 2.5 — run one iteration of the production state machine
    and persist trajectories.

    Returns the final tuple of ``QuestionStateInIteration`` objects. Side
    effects: writes ``qstate_<qid>.json`` per state and
    ``trajectory_<qid>.json`` per QID under ``run_root``.

    ``space_id`` and ``metadata_snapshot`` thread through to
    ``TransformerContext`` so ``applier_gate`` can actually call
    :func:`applier.apply_patch_set` (which validates ``metadata_snapshot``
    against the :class:`SerializedSpace` schema and PATCHes the Genie
    Space at ``/api/2.0/genie/spaces/{space_id}``). Production callers
    that operate against a live workspace already provide both; tests
    that drive the SM with a :class:`FakeWorkspaceClient` need them
    to advance past ``APPLYABLE`` to ``APPLIED``. The defaults match
    the pre-existing ``TransformerContext`` defaults (empty string,
    empty dict), so callers that did not previously pass these
    arguments are unaffected.

    Lazy imports throughout: this entry point is called from the lever
    loop alongside the legacy iteration; pulling the state_machine
    surfaces at module-load time would create the same circular
    reference the dispatch-input wrapper avoided in PR 1.1.
    """
    from pathlib import Path

    from genie_space_optimizer.optimization.state_machine.persistence import (
        write_qstate,
        write_trajectory,
    )
    from genie_space_optimizer.optimization.state_machine.registry import (
        build_production_state_machine,
    )
    from genie_space_optimizer.optimization.state_machine.trajectory import (
        build_trajectory,
    )
    from genie_space_optimizer.optimization.state_machine.verdict import (
        TransformerContext,
        ValidationContext,
    )

    run_root = Path(run_root)

    # Trial 15 — fail-fast invariant at the harness/SM seam. Any
    # iteration that may reach APPLIED needs either a real
    # ``eval_kwargs`` mapping or a workbench
    # ``extras['post_apply_eval']`` stub; without one of them
    # ``evaluated_gate._run_post_apply_eval`` calls
    # ``run_evaluation(**None)`` and every APPLIED state terminates
    # with ``OPTIMIZER_INVARIANT_VIOLATION post_apply_eval_failed``.
    # Catch the wiring drift here at the entry point rather than 8
    # transformers deep through a TypeError stack. This check runs
    # before the early-return for empty ``eval_rows`` so the same
    # ``ValueError`` shape catches misconfigured callers regardless of
    # whether the SM actually has work to do.
    _has_eval_kwargs = eval_kwargs is not None
    _has_post_apply_stub = (
        extras is not None
        and isinstance(extras, dict)
        and "post_apply_eval" in extras
    )
    # Pre-Trial-15 tests under ``tests/integration/`` and ``tests/unit/``
    # drive the SM through APPLIED without supplying ``eval_kwargs`` and
    # without a ``post_apply_eval`` stub. They were the silent baseline
    # for the production defect — they passed because ``evaluated_gate``
    # TypeError'd inside a broad ``except`` and the trajectory still
    # reached ``APPLIED`` (the assertion most of those tests check). The
    # env var below lets the test suite opt out of the invariant in one
    # place (``tests/conftest.py``) without modifying every test
    # callsite. Production never sets this var, so the harness path
    # remains strict.
    import os as _os_for_invariant
    _suppress_invariant = (
        _os_for_invariant.environ.get(
            "GSO_SM_TEST_ALLOW_MISSING_EVAL_CONTRACT", ""
        ).lower()
        in ("1", "true", "yes")
    )
    if (
        not _has_eval_kwargs
        and not _has_post_apply_stub
        and not _suppress_invariant
    ):
        raise ValueError(
            "SM_EVALUATOR_CONTRACT_MISSING: "
            "run_state_machine_iteration_and_persist was invoked without "
            "eval_kwargs and without an extras['post_apply_eval'] stub. "
            "evaluated_gate will fail with TypeError on any APPLIED state. "
            "Pass eval_kwargs from the harness (see harness.py "
            "_run_gate_checks for the canonical construction) or provide "
            "the workbench stub via extras['post_apply_eval']. Tests that "
            "intentionally do not reach the evaluated_gate may set "
            "GSO_SM_TEST_ALLOW_MISSING_EVAL_CONTRACT=1."
        )

    initial_states = _build_state_machine_initial_states(
        eval_rows=eval_rows, iteration=iteration,
    )
    if not initial_states:
        return ()

    # 2026-05-23 cutover Phase 1 — the diagnose / synthesize / gate
    # transformers need the same eval rows the dispatch admission used,
    # in order to find the per-qid evaluation row that grounds the LLM
    # prompt and that the validation gates inspect. Without this, every
    # SM Stage 1 transition terminated with ``no_eval_row_for_qid`` in
    # the 2026-05-23 trial (see runid 1084707218370768 / 538827243617302
    # postmortems). The tuple snapshot is intentional: the SM does not
    # mutate caller state.
    #
    # Trial 13 Phase 8 — single normalization boundary. Every eval row
    # entering the lever loop is projected into a
    # :class:`CanonicalEvalRow` exactly once, here. Downstream
    # consumers that still rely on the dict API see a Mapping shim
    # over ``raw``; new consumers should reach for the typed
    # attributes (``.qid``, ``.namespaced_qid``, ``.question_text``,
    # ``.judge_rationales``, ``.asi_metadata``). Future row-shape
    # extensions land in :func:`normalize_eval_row` exclusively; the
    # golden CI test enforces this invariant.
    from genie_space_optimizer.optimization.canonical_eval_row import (
        normalize_eval_row,
    )
    _baseline_eval_rows = tuple(
        normalize_eval_row(r) for r in (eval_rows or ())
    )
    # Trial 15 — start with the canonical extras dict and merge caller-
    # supplied entries (e.g. the workbench's deterministic
    # ``post_apply_eval`` stub) on top.
    _extras_seed: dict = {"workspace_client": workspace_client}
    if extras:
        for _k, _v in extras.items():
            _extras_seed[_k] = _v
    _ctx_kwargs: dict = {
        "iteration": iteration,
        "run_id": run_id,
        "validation_context": ValidationContext(
            iteration, run_id, {"workspace_client": workspace_client},
        ),
        "forbidden_signatures": forbidden_signatures,
        "extras": _extras_seed,
        "baseline_eval_rows": _baseline_eval_rows,
        "w": workspace_client,
        "space_id": space_id,
    }
    if metadata_snapshot is not None:
        # ``TransformerContext`` accepts a Mapping; preserving caller
        # references is fine because the applier deep-copies before
        # mutating. Falling through when ``None`` keeps the existing
        # ``default_factory=dict`` behaviour.
        _ctx_kwargs["metadata_snapshot"] = dict(metadata_snapshot)
    # Trial 13 typed-evidence cutover — surface Plan 12's per-QID typed
    # RCA evidence into the SM canonical lane via TransformerContext.
    # ``metadata_snapshot["_rca_evidence_typed"]`` is the established
    # carrier (Plan 11 batch path at ``_build_plan11_failing_qids_from_typed_evidence``
    # reads from the same key). Without this thread, the SM lane silently
    # dropped typed evidence at Stage 1 and aborted hard QIDs whose rows
    # alone did not carry blame/rca with
    # ``evidence_card_empty:blame_set_empty,rca_evidence_empty``.
    _rca_evidence_typed = (
        (metadata_snapshot or {}).get("_rca_evidence_typed") or {}
    )
    if _rca_evidence_typed:
        _ctx_kwargs["rca_evidence_typed"] = dict(_rca_evidence_typed)

    # Trial 13i — derive run-level ``schema_columns`` from the same
    # carriers the Plan 11 batch lane already consults
    # (``metadata_snapshot["schema_columns"]`` -> typed evidence union ->
    # identifier allowlist) so the SM canonical lane stops sending
    # ``"schema_columns": []`` to every Stage 1 LLM call. The post-Trial-13h
    # workbench replay surfaced QIDs declining with
    # ``insufficient_blame_set`` solely because the LLM had no schema list
    # to ground against.
    from genie_space_optimizer.optimization.schema_columns import (
        _derive_schema_columns,
    )
    _schema_columns_tuple, _schema_columns_source = _derive_schema_columns(
        metadata_snapshot=metadata_snapshot,
        rca_evidence_typed=_rca_evidence_typed or None,
        uc_columns=(metadata_snapshot or {}).get("uc_columns"),
    )
    _ctx_kwargs["schema_columns"] = _schema_columns_tuple
    _ctx_kwargs["schema_columns_source"] = _schema_columns_source

    # Trial 15 — plumb the evaluator boundary into the SM lane. Without
    # these fields ``evaluated_gate._run_post_apply_eval`` calls
    # ``run_evaluation(**None)`` and every APPLIED state terminates with
    # ``OPTIMIZER_INVARIANT_VIOLATION post_apply_eval_failed:TypeError``
    # (see runid_analysis/dc89d1a9 + 98ec8950 postmortems for the
    # gate-reasoning evidence).
    if stage_ctx is not None:
        _ctx_kwargs["stage_ctx"] = stage_ctx
    if eval_kwargs is not None:
        _ctx_kwargs["eval_kwargs"] = eval_kwargs
    if eval_qids:
        _ctx_kwargs["eval_qids"] = tuple(eval_qids)
    if baseline_eval is not None:
        _ctx_kwargs["baseline_eval"] = baseline_eval

    ctx = TransformerContext(**_ctx_kwargs)

    sm = build_production_state_machine()
    final_states = sm.run_iteration(initial_states, ctx)

    for s in final_states:
        write_qstate(run_root=run_root, state=s)
        traj = build_trajectory(qid=s.qid, iterations=(s,))
        write_trajectory(run_root=run_root, trajectory=traj)

    return final_states


def _os_env_run_root() -> str:
    """Operator override for the canary's trajectory output root.

    Resolution order:
      1. GSO_PLAN_V3_RUN_ROOT (explicit operator override)
      2. GSO_PHASE_H_BUNDLE_ROOT (default; postmortem bundle assembler reads this)
      3. "" (caller falls back to /tmp/gso/<run_id> — non-durable; only for tests)
    """
    import os as _os
    explicit = _os.environ.get("GSO_PLAN_V3_RUN_ROOT", "")
    if explicit:
        return explicit
    return _os.environ.get("GSO_PHASE_H_BUNDLE_ROOT", "")


def _build_canary_stage_ctx_and_eval_kwargs(
    *, run_id, iteration, space_id, domain, catalog, schema,
    phase_h_anchor_run_id, w, spark, exp_name, benchmarks,
    predict_fn, scorers, reference_sqls, uc_schema,
    max_benchmark_count,
):
    """Construct minimal-but-real ``StageContext`` + ``RunEvaluationKwargs``
    for the canary's evaluated_gate, mirroring ``_run_gate_checks``'s
    F1 setup at harness.py:16368–16404.

    Lazy import to avoid pulling stages package at module load.
    """
    from genie_space_optimizer.optimization.stages import (
        RunEvaluationKwargs,
        StageContext,
    )
    stage_ctx = StageContext(
        run_id=str(run_id),
        iteration=int(iteration),
        space_id=str(space_id),
        domain=str(domain),
        catalog=str(catalog),
        schema=str(schema),
        apply_mode="real",
        journey_emit=lambda *a, **k: None,
        decision_emit=lambda record: None,
        mlflow_anchor_run_id=phase_h_anchor_run_id,
        feature_flags={},
    )
    eval_kwargs: RunEvaluationKwargs = {
        "space_id": str(space_id),
        "experiment_name": str(exp_name),
        "iteration": int(iteration),
        "benchmarks": benchmarks,
        "domain": str(domain),
        "model_id": None,
        "eval_scope": "full",
        "predict_fn": predict_fn,
        "scorers": scorers,
        "spark": spark,
        "w": w,
        "catalog": str(catalog),
        "gold_schema": str(schema),
        "uc_schema": str(uc_schema),
        "reference_sqls": reference_sqls if reference_sqls else None,
        "model_creation_kwargs": {},
        "max_benchmark_count": int(max_benchmark_count or 0),
        "run_name": f"canary_eval_iter_{int(iteration):03d}",
        "extra_tags": {},
    }
    return stage_ctx, eval_kwargs


def maybe_run_state_machine_canary_iteration(
    *,
    eval_rows,
    iteration: int,
    run_id: str,
    workspace_client=None,
    space_id: str = "",
    metadata_snapshot=None,
    forbidden_signatures: tuple[str, ...] = (),
    # Plumbed-from-harness fields for evaluated_gate / applier_gate
    # to operate against real state. All optional so tests can still
    # build minimal contexts.
    domain: str = "",
    catalog: str = "",
    schema: str = "",
    phase_h_anchor_run_id=None,
    spark=None,
    exp_name: str = "",
    benchmarks=None,
    predict_fn=None,
    scorers=None,
    reference_sqls=None,
    uc_schema: str = "",
    max_benchmark_count: int = 0,
) -> tuple:
    """Plan v3 — historical per-iteration canary callsite (no-op).

    The umbrella flag ``plan_v3_state_machine_iteration_enabled`` was
    deleted in Phase 4: the state machine is now authoritative and the
    harness drives it directly (see Task 4.1). This helper is preserved
    only so legacy callsites and dependent imports continue to type-
    check; it does no real work.

    (Harness no longer calls this; preserved for tests until Phase 5.)
    """
    return ()

from genie_space_optimizer.common.config import (
    ADAPTIVE_STRATEGIST_PROMPT,
    APPLY_MODE,
    CONFLICT_RULES,
    DEFAULT_THRESHOLDS,
    DESCRIPTION_ENRICHMENT_PROMPT,
    ENABLE_RCA_EXAMPLE_SQL_SYNTHESIS,
    ENABLE_RCA_JOIN_SPEC_BRIDGE,
    ENABLE_RCA_SQL_SNIPPET_BRIDGE,
    ENABLE_RCA_THEMES_STRATEGIST,
    FAILURE_TAXONOMY,
    GENERIC_FIX_PREFIXES,
    INSTRUCTION_SECTION_ORDER,
    LEVER_TO_SECTIONS,
    LEVER_1_2_COLUMN_PROMPT,
    LEVER_4_JOIN_DISCOVERY_PROMPT,
    LEVER_4_JOIN_SPEC_PROMPT,
    LEVER_5_HOLISTIC_PROMPT,
    LEVER_5_INSTRUCTION_PROMPT,
    LEVER_6_SQL_EXPRESSION_PROMPT,
    LEVER_NAMES,
    LLM_ENDPOINT,
    LLM_MAX_RETRIES,
    LLM_TEMPERATURE,
    LOW_RISK_PATCHES,
    MAX_ACTION_GROUPS_PER_STRATEGY,
    MAX_HOLISTIC_INSTRUCTION_CHARS,
    MAX_PATCH_OBJECTS,
    MAX_VALUE_DICTIONARY_COLUMNS,
    MEDIUM_RISK_PATCHES,
    PATCH_TYPES,
    PROMPT_TOKEN_BUDGET,
    PROPOSAL_GENERATION_PROMPT,
    REGRESSION_THRESHOLD,
    REPEATABILITY_FIX_BY_ASSET,
    SAMPLE_QUESTIONS_PROMPT,
    SPACE_DESCRIPTION_PROMPT,
    STRATEGIST_DETAIL_PROMPT,
    STRATEGIST_PROMPT,
    STRATEGIST_TRIAGE_PROMPT,
    _LEVER_TO_PATCH_TYPE,
    format_mlflow_template,
)
from genie_space_optimizer.common.genie_schema import ensure_join_spec_fields

logger = logging.getLogger(__name__)

_LLM_TIMEOUT_SECONDS = 600
_EXAMPLE_SQL_SIMILARITY_THRESHOLD = 0.85
_INSTR_LOSS_THRESHOLD = 0.75


def _ws_with_timeout(
    w: WorkspaceClient | None,
    timeout: int = _LLM_TIMEOUT_SECONDS,
) -> WorkspaceClient:
    """Return a **new** workspace client whose HTTP session uses *timeout*.

    The Databricks SDK bakes ``http_timeout_seconds`` into the
    ``requests.Session`` at construction time, so mutating the config
    after the fact has no effect.  We therefore always create a fresh
    client.  In a Databricks job the env-var auth (``DATABRICKS_HOST``,
    ``DATABRICKS_TOKEN``, etc.) is inherited automatically.

    When *w* is provided its config is cloned using ``Config.attributes()``
    (the SDK's own attribute registry) and the original credentials strategy,
    so every auth method — PAT, OAuth M2M, Azure MI, Google SA, etc. — is
    preserved automatically.
    """
    from databricks.sdk.config import Config

    if w is None:
        return make_workspace_client(config=Config(http_timeout_seconds=timeout))

    cfg_kwargs: dict[str, Any] = {}
    for attr in Config.attributes():
        val = getattr(w.config, attr.name, None)
        if val is not None:
            cfg_kwargs[attr.name] = val
    cfg_kwargs["http_timeout_seconds"] = timeout

    return make_workspace_client(
        config=Config(
            credentials_strategy=w.config._credentials_strategy,
            **cfg_kwargs,
        )
    )


def _estimate_tokens(text: str) -> int:
    """Conservative token estimate (~4 chars per token)."""
    return len(text) // 4


def _truncate_to_budget(
    format_kwargs: dict[str, Any],
    prompt_template: str,
    priority_keys: list[str],
) -> dict[str, Any]:
    """Truncate low-priority context sections to fit within PROMPT_TOKEN_BUDGET.

    ``priority_keys`` lists context keys from LOWEST to HIGHEST priority.
    When the estimated prompt exceeds the budget, the lowest-priority keys
    are truncated first (keeping a summary prefix).
    """
    est = _estimate_tokens(prompt_template) + sum(
        _estimate_tokens(str(v)) for v in format_kwargs.values()
    )
    if est <= PROMPT_TOKEN_BUDGET:
        return format_kwargs

    overshoot = est - PROMPT_TOKEN_BUDGET
    result = dict(format_kwargs)

    for key in priority_keys:
        if overshoot <= 0:
            break
        val = str(result.get(key, ""))
        if not val:
            continue
        char_budget = max(200, len(val) - overshoot * 4)
        if char_budget < len(val):
            truncated = val[:char_budget]
            result[key] = truncated + f"\n... ({len(val) - char_budget} chars truncated for token budget)"
            overshoot -= _estimate_tokens(val) - _estimate_tokens(result[key])

    return result


# ── Traced LLM Call Helper ─────────────────────────────────────────────
#
# _resolve_bearer_token, _get_openai_client, and _openai_client_cache
# now live in ``llm_client.py`` and are re-imported at the top of this
# module for backward compatibility with tests and other consumers.


def _attach_last_response(exc: BaseException, text: str) -> None:
    """Stamp the last LLM body onto an exception for downstream logging.

    When ``_traced_llm_call`` exhausts retries, callers need to know
    *what the model actually returned* — not just that parsing failed.
    Attaching via attribute (vs. wrapping the exception) preserves the
    original type/traceback so existing ``except`` chains and MLflow
    span events keep working unchanged.

    The attributes are best-effort: if ``exc`` is a frozen / C-level
    exception that rejects attribute assignment, we swallow the
    AttributeError silently (the caller falls back to an empty preview).
    """
    try:
        exc.last_response_text = text  # type: ignore[attr-defined]
        exc.last_response_chars = len(text)  # type: ignore[attr-defined]
    except (AttributeError, TypeError):
        pass


# ──────────────────────────────────────────────────────────────────────
# Phase 3 (2026-05-17) — Lever Loop Tape Replay
#
# ``_LLM_CALLER_OVERRIDE`` lets tests intercept every LLM call routed
# through ``_traced_llm_call`` without changing any call-site signature.
# Production callers see no behavioral change: ``ContextVar.get()``
# returns ``None`` by default and the legacy code path executes.
#
# Replay tests set the override at the lever-loop entry boundary via
# ``LeverLoopReplayHarness`` (tape_replay_harness.py) and reset it on
# exit. Concurrency: ContextVar isolates state per asyncio task /
# thread, matching how MLflow tracing manages its own context.
# ──────────────────────────────────────────────────────────────────────
from contextvars import ContextVar as _Phase3ContextVar
from typing import Protocol as _Phase3Protocol


class LLMCallerOverride(_Phase3Protocol):
    """Protocol for an installable LLM caller that bypasses the real LLM."""

    def call(
        self,
        *,
        w: Any,
        system_msg: str,
        prompt: str,
        span_name: str,
        max_retries: int,
        temperature: float,
        max_tokens: int | None,
        response_validator: Callable[[str], Any] | None,
        response_format: dict[str, Any] | None,
        response_model: type | None,
    ) -> tuple[str, Any]:
        ...


_LLM_CALLER_OVERRIDE: _Phase3ContextVar[LLMCallerOverride | None] = (
    _Phase3ContextVar("_LLM_CALLER_OVERRIDE", default=None)
)


# ──────────────────────────────────────────────────────────────────────
# Phase 3.5 (2026-05-17) — production LLM call recorder.
#
# ``_LLM_CALL_RECORDER`` is the capture-side counterpart to Phase 3's
# ``_LLM_CALLER_OVERRIDE``. Production runs install an
# ``InMemoryLLMCallRecorder`` at the lever-loop boundary; every
# successful LLM call routed through ``_traced_llm_call`` appends to
# it. The override path (replay) explicitly skips capture to avoid a
# feedback loop.
# ──────────────────────────────────────────────────────────────────────
from genie_space_optimizer.optimization.llm_call_recorder import (
    LLMCallRecorder as _LLMCallRecorderType,
)

_LLM_CALL_RECORDER: _Phase3ContextVar[_LLMCallRecorderType | None] = (
    _Phase3ContextVar("_LLM_CALL_RECORDER", default=None)
)


def _safe_token_count(response: Any, field: str) -> int | None:
    """Return ``response.usage.<field>`` or ``None`` on any failure.

    Used by the Phase 3.5 recorder to populate ``response_metadata``
    without crashing on response shapes that lack ``usage``.
    """
    try:
        usage = getattr(response, "usage", None)
        if usage is None:
            return None
        v = getattr(usage, field, None)
        return int(v) if v is not None else None
    except Exception:
        return None


def _traced_llm_call(
    w: WorkspaceClient | None,
    system_msg: str,
    prompt: str,
    *,
    span_name: str,
    max_retries: int = LLM_MAX_RETRIES,
    temperature: float = LLM_TEMPERATURE,
    max_tokens: int | None = None,
    response_validator: Callable[[str], Any] | None = None,
    response_format: dict[str, Any] | None = None,
    response_model: type | None = None,
) -> tuple[str, Any]:
    """Execute an LLM call via the OpenAI SDK with automatic MLflow tracing.

    ``mlflow.openai.autolog()`` instruments every OpenAI call with a
    ``CHAT_MODEL`` span that captures token usage, cost, and latency.
    This wrapper adds retry logic inside a ``CHAIN`` span and logs
    token usage on the span for visibility.

    Returns ``(raw_text, response_object)`` for the caller to parse.
    Raises the last exception if all retries are exhausted.

    ``response_validator`` (optional): a callable invoked with the
    trimmed completion text after each successful HTTP round-trip. If
    it raises, the exception is treated as a retryable failure (same
    exponential backoff as RPC failures). This closes the gap where a
    provider returns HTTP 200 with non-JSON / refusal content that
    callers downstream cannot parse — most notably
    ``_extract_json`` — and that previously surfaced as two identical
    tracebacks with no retry attempt. Callers that expect JSON can
    pass ``response_validator=_extract_json``. When ``None`` the
    legacy behaviour (return first HTTP 200, no post-success
    validation) is preserved for every existing call site.
    """
    # Phase 3 (2026-05-17) — consult tape-replay override before any
    # LLM/MLflow work. The override is a ContextVar set by
    # LeverLoopReplayHarness — it returns deterministic (text,
    # response) from a tape and skips OpenAI/MLflow entirely.
    _override = _LLM_CALLER_OVERRIDE.get()
    if _override is not None:
        _local_response_format = response_format
        _local_response_validator = response_validator
        if response_model is not None:
            from genie_space_optimizer.optimization.prompt_io import (
                build_response_format,
                validate_and_parse,
            )
            if _local_response_format is None:
                _local_response_format = build_response_format(response_model)
            if _local_response_validator is None:
                _local_response_validator = (
                    lambda txt: validate_and_parse(txt, response_model)  # noqa: E731
                )
        return _override.call(
            w=w,
            system_msg=system_msg,
            prompt=prompt,
            span_name=span_name,
            max_retries=max_retries,
            temperature=temperature,
            max_tokens=max_tokens,
            response_validator=_local_response_validator,
            response_format=_local_response_format,
            response_model=response_model,
        )

    import time

    import mlflow
    from mlflow.entities import SpanEvent, SpanType

    # Plan 2026-05-17-prompt-registry-and-typed-io-hygiene Task 12 —
    # derive response_format + response_validator from a Pydantic
    # response_model when the caller provides one.
    if response_model is not None:
        from genie_space_optimizer.optimization.prompt_io import (
            build_response_format,
            validate_and_parse,
        )
        if response_format is None:
            response_format = build_response_format(response_model)
        if response_validator is None:
            response_validator = lambda txt: validate_and_parse(txt, response_model)  # noqa: E731

    with mlflow.start_span(name=span_name, span_type=SpanType.CHAIN) as span:
        # Phase 3.6 (2026-05-17) — read recorder binding so historic-
        # style extractors don't need timestamp-correlation hacks.
        # Default RecorderBinding values (-1 / "") are recorded
        # explicitly so the schema is uniform whether or not the
        # lever loop is bound.
        from genie_space_optimizer.optimization.llm_call_recorder import (
            _RECORDER_BINDING as _phase36_binding_ctx,
        )

        _phase36_binding = _phase36_binding_ctx.get()
        span.set_inputs({
            "model": LLM_ENDPOINT,
            "temperature": temperature,
            "prompt_chars": len(prompt),
            "response_model": response_model.__name__ if response_model else None,
            # Phase 3.6 breadcrumbs — read by mlflow_trace_extractor at
            # tape-capture time so historic tapes can be keyed by
            # iteration/ag_id/cluster_id without timestamp inference.
            "iteration": int(_phase36_binding.iteration),
            "ag_id": str(_phase36_binding.ag_id),
            "cluster_id": str(_phase36_binding.cluster_id),
        })

        client = _get_openai_client(w)
        text = ""
        # F6 — track the most recent HTTP 200 body across attempts so we
        # can attach it to the raised exception if every retry ends in
        # validator/RPC failure. Callers (description enrichment, etc.)
        # read this off the exception to log a structured preview of
        # *what the model actually returned* rather than an empty string.
        last_response_text: str = ""
        last_err: Exception | None = None

        for attempt in range(max_retries):
            try:
                messages: list[dict[str, str]] = []
                if system_msg and system_msg.strip():
                    messages.append({"role": "system", "content": system_msg})
                messages.append({"role": "user", "content": prompt})
                call_kwargs: dict[str, Any] = {
                    "model": LLM_ENDPOINT,
                    "messages": messages,
                    "temperature": temperature,
                }
                if max_tokens is not None:
                    call_kwargs["max_tokens"] = max_tokens
                if response_format is not None:
                    call_kwargs["response_format"] = response_format

                # PR-2C — local pre-flight against the Databricks
                # endpoint contract. The dc89d1a9 / 98ec8950 trials
                # showed every Plan 11 Stage 1 call rejected at the
                # wire with ``tools.0.custom.name failed
                # ^[a-zA-Z0-9_-]{1,128}$`` because the response_format
                # name carried ``[`` and ``]``. PR-1B sanitizes the
                # name; PR-2C catches every *future* envelope-shape
                # mismatch locally so a malformed request never burns
                # the retry budget or pollutes MLflow with 400s.
                #
                # Raises ``RequestEnvelopeInvalidError`` (typed) on
                # any violation; ``_classify_llm_error`` routes the
                # class name to ``error_kind="request_envelope_invalid"``
                # automatically. The retry loop's ``except`` clause
                # detects this class and breaks out — retrying a
                # deterministically-broken envelope is wasted budget.
                _preflight_violations = (
                    _DATABRICKS_REQUEST_CONTRACT.validate(call_kwargs)
                )
                if _preflight_violations:
                    raise RequestEnvelopeInvalidError(_preflight_violations)

                response = client.chat.completions.create(**call_kwargs)

                if not response.choices:
                    raise ValueError("LLM response had no choices")
                content = response.choices[0].message.content
                if not content:
                    raise ValueError("LLM response content is empty")
                text = str(content).strip()
                last_response_text = text

                if response_validator is not None:
                    try:
                        response_validator(text)
                    except Exception as exc:
                        last_err = exc
                        span.add_event(SpanEvent(
                            name=f"validator_reject_attempt_{attempt + 1}",
                            attributes={
                                "error": str(exc)[:500],
                                "response_preview": text[:200],
                                "response_chars": len(text),
                            },
                        ))
                        if attempt < max_retries - 1:
                            time.sleep(2**attempt)
                            continue
                        # F6 — attach the last-seen body to the
                        # exception so the caller's warning can log a
                        # real preview instead of an empty string.
                        _attach_last_response(exc, last_response_text)
                        raise

                _log_token_usage(span, response)

                span.set_outputs({
                    "response_chars": len(text),
                    "attempts": attempt + 1,
                })

                # Phase 3.5 — best-effort capture of every successful LLM
                # call. Override path (Phase 3 replay) does not reach
                # here, so this fires only on real LLM calls.
                _recorder = _LLM_CALL_RECORDER.get()
                if _recorder is not None:
                    try:
                        _recorder.record(
                            span_name=span_name,
                            system_msg=system_msg,
                            prompt=prompt,
                            response_text=text,
                            response_metadata={
                                "model": LLM_ENDPOINT,
                                "temperature": float(temperature),
                                "max_tokens": (
                                    int(max_tokens)
                                    if max_tokens is not None
                                    else None
                                ),
                                "prompt_tokens": _safe_token_count(
                                    response, "prompt_tokens",
                                ),
                                "completion_tokens": _safe_token_count(
                                    response, "completion_tokens",
                                ),
                                "total_tokens": _safe_token_count(
                                    response, "total_tokens",
                                ),
                            },
                        )
                    except Exception:
                        logger.debug(
                            "Phase 3.5: LLM call recorder raised "
                            "(non-fatal)",
                            exc_info=True,
                        )

                return text, response

            except RequestEnvelopeInvalidError as exc:
                # PR-2C — local pre-flight refused to dispatch. The
                # envelope is deterministically malformed; retrying
                # the same call_kwargs would produce the same violation
                # list. Break out immediately so callers see one clean
                # ``request_envelope_invalid`` classification rather
                # than ``max_retries`` identical retry events.
                last_err = exc
                span.add_event(SpanEvent(
                    name="request_envelope_invalid",
                    attributes={
                        "error": str(exc)[:500],
                        "constraint_violations": [
                            f"{v.field}|{v.constraint}"
                            for v in exc.violations
                        ],
                    },
                ))
                break
            except Exception as exc:
                last_err = exc
                span.add_event(SpanEvent(
                    name=f"retry_attempt_{attempt + 1}",
                    attributes={"error": str(exc)[:500]},
                ))
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)

        span.set_outputs({
            "error": str(last_err)[:500] if last_err else "unknown",
            "attempts": max_retries,
        })
        # F6 — attach before re-raise on the non-validator exhaustion
        # path too (HTTP failures, empty-choice responses, etc.).
        if last_err is not None:
            _attach_last_response(last_err, last_response_text)
        raise last_err  # type: ignore[misc]


def _adaptive_strategist_response_validator(text: str) -> Any:
    """Tolerant JSON validator for the adaptive strategist call.

    Strategy:
    1. Try strict parse via ``_extract_json``.
    2. On ``json.JSONDecodeError`` fall back to
       ``_repair_truncated_strategy_json`` — the same salvage path used at
       the call site post-success. This stops truncated-but-recoverable
       responses from being treated as non-JSON refusals and exhausting
       retries inside ``_traced_llm_call``.
    3. Re-raise the original ``JSONDecodeError`` only if BOTH parses fail.
    """
    from genie_space_optimizer.optimization.evaluation import _extract_json

    try:
        # ``strict=True`` preserves the legacy raise-on-error behavior so
        # ``_traced_llm_call`` can retry true non-JSON refusals.
        return _extract_json(text, strict=True)
    except json.JSONDecodeError:
        try:
            return _repair_truncated_strategy_json(text)
        except json.JSONDecodeError:
            raise


def _log_token_usage(span: Any, response: Any) -> None:
    """Attach token usage from an OpenAI response to an MLflow span."""
    usage = getattr(response, "usage", None)
    if not usage:
        return
    try:
        span.set_attribute("mlflow.chat.tokenUsage", {
            "input_tokens": getattr(usage, "prompt_tokens", 0) or 0,
            "output_tokens": getattr(usage, "completion_tokens", 0) or 0,
            "total_tokens": getattr(usage, "total_tokens", 0) or 0,
        })
    except Exception:
        pass


def _get_existing_example_sqls(metadata_snapshot: dict) -> list:
    """Extract example_question_sqls from the correct config path.

    The Genie API stores these under ``instructions.example_question_sqls``,
    not at the top level.  Falls back through several layouts to handle
    both parsed-space dicts and wrapper dicts.
    """
    instr = metadata_snapshot.get("instructions", {})
    if isinstance(instr, dict):
        eqs = instr.get("example_question_sqls", [])
        if eqs:
            return eqs
    cfg = metadata_snapshot.get("config")
    if isinstance(cfg, dict):
        instr2 = cfg.get("instructions", {})
        if isinstance(instr2, dict):
            eqs2 = instr2.get("example_question_sqls", [])
            if eqs2:
                return eqs2
    return []


def _row_qid(row: dict, *, fallback: str = "unknown") -> str:
    """Extract question_id from an eval-results row regardless of column layout.

    MLflow stores inputs in the ``request`` column (not ``inputs/…``), so we
    parse both layouts to robustly recover the QID.
    """
    direct = (
        row.get("inputs/question_id")
        or row.get("inputs/question")
        or row.get("question_id")
    )
    if direct:
        return str(direct)
    _req = row.get("request") or {}
    if isinstance(_req, str):
        try:
            _req = json.loads(_req)
        except (json.JSONDecodeError, TypeError):
            _req = {}
    if isinstance(_req, dict):
        _kw = _req.get("kwargs", {})
        qid = _kw.get("question_id") or _req.get("question_id") or _req.get("question")
        if qid:
            return str(qid)
    return row.get("question", fallback) or fallback


# ── Dual Persistence Paths ─────────────────────────────────────────────

DUAL_PERSIST_PATHS: dict[int, dict[str, str]] = {
    1: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "gold_layer_design/yaml/{domain}/*.yaml",
    },
    2: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/semantic/metric_views/*.yaml",
    },
    3: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/semantic/tvfs/*.sql",
    },
    4: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/genie/{domain}_genie_export.json",
    },
    5: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/genie/{domain}_genie_export.json",
    },
    6: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/genie/{domain}_genie_export.json",
    },
}


# ═══════════════════════════════════════════════════════════════════════
# 1. Failure-to-Lever Mapping (pure)
# ═══════════════════════════════════════════════════════════════════════


_JUDGE_TO_LEVER: dict[str, int] = {
    "schema_accuracy": 1,
    "syntax_validity": 1,
    "completeness": 2,
    "logical_accuracy": 2,
    "semantic_equivalence": 2,
    "result_correctness": 2,
    "asset_routing": 5,
}


# Module-level lever map. Extracted from ``_map_to_lever`` so tests and other
# call sites can introspect the routing table without invoking the function.
# ``format_difference`` routes to Lever 5: the arbiter saw two SQL shapes that
# produced similar results but didn't match the expected canonical form.
# Surfacing the expected_sql as an example_sql teaches the canonical shape.
_ROOT_CAUSE_LEVER_MAP: dict[str, int] = {
    "wrong_column": 1,
    "wrong_table": 1,
    "description_mismatch": 1,
    "missing_synonym": 1,
    # Phase A1: SQL-shape aggregation/filter causes now route to Lever 6
    # (sql_snippet_* primitives). Lever 2 could only update MV descriptions,
    # which cannot add a measure, filter, or dimension. Example routing
    # lookup: missing_filter -> sql_snippet_filter, wrong_aggregation ->
    # sql_snippet_measure.
    "wrong_aggregation": 6,
    "wrong_measure": 6,
    "missing_filter": 6,
    "missing_scd_filter": 6,
    "wrong_filter_condition": 6,
    "missing_temporal_filter": 6,
    "wrong_join_type": 5,
    "tvf_parameter_error": 3,
    "wrong_join": 4,
    "missing_join_spec": 4,
    "wrong_join_spec": 4,
    "asset_routing_error": 5,
    "missing_instruction": 5,
    "ambiguous_question": 5,
    # v2 Task 7: format_difference now routes to Lever 5 with a templated
    # example_sql body so the strategist has a concrete patch to emit.
    # Was 0 (no lever) which left the strategist without any actionable
    # routing for canonical-shape mismatches.
    "format_difference": 5,
    "extra_columns_only": 0,
    "select_star": 0,
    "missing_dimension": 6,
    "wrong_grouping": 6,
    # P1 pattern labels (Phase 2). Without explicit entries here these
    # labels fall through to ``_JUDGE_TO_LEVER[judge]``, which routes
    # plural_top_n_collapse / granularity_drop / time_window_pivot to
    # Lever 2 (Metric Views) on logical_accuracy failures. Lever 2 can
    # only update MV descriptions and cannot reshape SQL — exactly the
    # pathology the Phase A1 reroute fixed for wrong_aggregation et al.
    # Routing the SQL-shape patterns to Lever 5 lets the structural gate
    # force example_sql synthesis. Filter literal mismatches go to
    # Lever 6 (sql_snippet_filter); column ambiguity goes to Lever 1
    # (column synonyms / descriptions).
    "plural_top_n_collapse": 5,
    "time_window_pivot": 5,
    "granularity_drop": 5,
    "value_format_mismatch": 6,
    "column_disambiguation": 1,
}


def _format_difference_example_sql_template(
    *,
    question: str,
    expected_sql: str,
) -> str:
    """Render a Lever-5 example_sql body for a format_difference cluster.

    format_difference means the arbiter saw two SQL shapes that produced
    similar results but didn't match the expected canonical form. Surfacing
    the expected_sql as a teaching example lets the strategist learn the
    canonical shape without rewriting the user's question.
    """
    return (
        "EXAMPLE SQL — canonical shape for this question family:\n"
        f"-- Question: {question.strip()}\n"
        f"{expected_sql.strip()}\n"
    )


def _build_rca_forced_instruction_body(
    *,
    root_cause: str,
    grounding_terms: list[str],
    question: str,
    expected_sql: str,
) -> str | None:
    """Render the RCA-forced instruction body keyed on structured root_cause.

    Returns None for non-actionable root causes; the caller skips emission.
    Replaces the legacy substring-keyed dispatch (``"rank" in joined`` etc.)
    that produced incorrect bodies for column-disambiguation clusters.
    """
    if root_cause == "plural_top_n_collapse":
        return (
            "QUERY PATTERNS:\n"
            "- For plural highest/lowest ranking questions, return one ranked row "
            "per requested entity sorted by the metric. Do not collapse the result "
            "to a single top-1 row unless the user explicitly asks for one entity."
        )
    if root_cause == "time_window_pivot":
        return (
            "QUERY PATTERNS:\n"
            "- When comparing day vs MTD metrics, query each time window separately "
            "and pivot the results into side-by-side columns at the requested grain. "
            "Do not return one row per time window unless the user asks for that."
        )
    if root_cause == "missing_temporal_filter":
        return (
            "QUERY PATTERNS:\n"
            "- For 'last N days/weeks/months' questions, restrict to rows whose "
            "dated column falls within the last N days/weeks/months relative to "
            "today. Do not rely on prose like 'recent' or 'last_30_days' as a "
            "column value."
        )
    if root_cause == "column_disambiguation":
        terms = ", ".join(t for t in grounding_terms if t)[:200]
        return (
            "COLUMN DISAMBIGUATION:\n"
            f"- The terms [{terms}] map to specific columns. Use the canonical column "
            "for each business term as documented in the column descriptions; do not "
            "substitute a different column when the description disagrees."
        )
    if root_cause == "missing_filter":
        return (
            "QUERY PATTERNS:\n"
            "- The expected answer requires a filter that is not in the user's prose. "
            "Add the documented default filter (see column description) before "
            "aggregating."
        )
    if root_cause == "format_difference":
        if not expected_sql or not question:
            return None
        return (
            "EXAMPLE SQL — canonical shape for this question family:\n"
            f"-- Question: {question.strip()}\n"
            f"{expected_sql.strip()}\n"
        )
    return None


def _map_to_lever(
    root_cause: str,
    asi_failure_type: str | None = None,
    blame_set: list | str | None = None,
    judge: str | None = None,
) -> int:
    """Map a failure root cause to its primary control lever (1-6).

    ASI ``failure_type`` takes precedence when present since it comes
    directly from the FAILURE_TAXONOMY and is more precise.
    Falls back to the judge name when rationale-based pattern extraction
    yields "other".
    """
    ft = (asi_failure_type if asi_failure_type and asi_failure_type != "other" else None) or root_cause

    if ft == "repeatability_issue":
        bs = str(blame_set).upper() if blame_set else ""
        return 5 if "TVF" in bs else 1

    # RCA-themed planning may override this coarse map. This fallback is
    # only for clusters that have no typed RCA findings.
    mapping = _ROOT_CAUSE_LEVER_MAP

    if asi_failure_type and asi_failure_type in mapping:
        return mapping[asi_failure_type]
    if root_cause in mapping:
        return mapping[root_cause]
    if judge and judge in _JUDGE_TO_LEVER:
        return _JUDGE_TO_LEVER[judge]
    return 5


def _resolve_scope(lever: int, apply_mode: str = APPLY_MODE) -> str:
    """Determine where a patch is applied based on lever and apply_mode.

    Levers 4-6 are always ``genie_config`` (Genie Space native structures).
    Levers 1-3 are governed by ``apply_mode``.
    """
    if lever in (4, 5, 6):
        return "genie_config"
    return apply_mode


# ═══════════════════════════════════════════════════════════════════════
# 2. Failure Clustering (pure)
# ═══════════════════════════════════════════════════════════════════════


_PATTERN_FILTER_MISSING = re.compile(
    r"\b(missing|no|without|lack(?:ing|s)?|absent|should\s+(?:be|have)\s+(?:had\s+)?a?)\s+"
    r"(?:\w+\s+){0,3}(filter|where|restriction|predicate|where\s+clause)\b",
    re.I,
)
_PATTERN_FILTER_WRONG = re.compile(
    r"\b(wrong|incorrect|bad|mistaken|flawed)\s+(?:\w+\s+){0,3}(filter|where|predicate)\b",
    re.I,
)
_PATTERN_JOIN_WRONG = re.compile(
    r"\b(wrong|incorrect|missing|bad|without)\s+(?:\w+\s+){0,3}join\b",
    re.I,
)
_PATTERN_JOIN_APPEARS_WRONG = re.compile(
    r"\bjoin\b[^.]{0,80}\b(is|are|appears|seem(?:s)?|looks?)\s+(?:to\s+be\s+)?"
    r"(wrong|incorrect|bad|mistaken)\b",
    re.I,
)
_PATTERN_JOIN_TYPE = re.compile(
    r"\b(left|right|inner|cross|full)\s+(?:outer\s+)?join\b.*?"
    r"(join\s+type|instead\s+of|wrong\s+join)",
    re.I | re.S,
)
_PATTERN_TABLE = re.compile(
    r"\b(wrong|missing|incorrect|bad)\s+(?:\w+\s+){0,3}table\b",
    re.I,
)
_PATTERN_COLUMN = re.compile(
    r"\b(wrong|missing|incorrect|bad)\s+(?:\w+\s+){0,3}column\b",
    re.I,
)
_PATTERN_AGG = re.compile(
    r"\b(wrong|missing|incorrect|bad)\s+(?:\w+\s+){0,3}(aggregation|measure|metric)\b",
    re.I,
)
_PATTERN_ROUTING = re.compile(
    r"\b(wrong|missing|incorrect)\s+(?:\w+\s+){0,3}(asset|routing|example)\b",
    re.I,
)
_PATTERN_INSTRUCTION = re.compile(
    r"\b(missing|unclear|incomplete|ambiguous)\s+(?:\w+\s+){0,3}(instruction|guidance)\b",
    re.I,
)


def _extract_pattern(rationale: str) -> str:
    """Extract a generalizable pattern from a judge rationale string.

    S3 hardening: judge rationales routinely contain words like
    ``"filter"`` or ``"where"`` in affirmative contexts
    (e.g. ``"filter is applied correctly"``), which the old substring
    matcher misclassified as ``missing_filter``. Each branch now
    requires both a noun (``filter``, ``where``, ``join``, ...) AND a
    failure adjective/verb (``missing``, ``wrong``, ``incorrect``, ...)
    within a short window. Bare mentions fall through to ``"other"`` so
    downstream SQL-diff classification runs instead of emitting noise.
    """
    r = (rationale or "").strip()
    if not r:
        return "other"
    rl = r.lower()

    if "correctly" in rl or "correct" in rl and "incorrect" not in rl:
        affirmative_only = re.search(
            r"\b(filter|where|join|column|table|aggregation|measure)\b[^.]{0,40}\b"
            r"(is|are|was|were)\s+(applied\s+)?(correct(ly)?|right)\b",
            rl,
        )
        if affirmative_only and not re.search(
            r"\b(missing|wrong|incorrect|no\s+where|no\s+filter|absent)\b", rl
        ):
            return "other"

    if "is_current" in rl and re.search(
        r"\b(missing|wrong|without|absent|no)\b", rl
    ):
        return "missing_scd_filter"
    if "scd" in rl and re.search(r"\b(filter|dimension)\b", rl) and re.search(
        r"\b(missing|wrong|without|absent|no)\b", rl
    ):
        return "missing_scd_filter"

    if _PATTERN_JOIN_TYPE.search(rl):
        return "wrong_join_type"
    if _PATTERN_TABLE.search(rl):
        return "wrong_table"
    if _PATTERN_COLUMN.search(rl):
        return "wrong_column"
    if _PATTERN_AGG.search(rl):
        return "wrong_aggregation"
    if _PATTERN_JOIN_WRONG.search(rl) or _PATTERN_JOIN_APPEARS_WRONG.search(rl):
        return "wrong_join"
    if _PATTERN_FILTER_MISSING.search(rl):
        return "missing_filter"
    if _PATTERN_FILTER_WRONG.search(rl):
        return "wrong_filter_condition"
    if re.search(r"\b(missing|without)\s+limit\b", rl):
        return "wrong_filter_condition"
    if _PATTERN_ROUTING.search(rl):
        return "asset_routing_error"
    if _PATTERN_INSTRUCTION.search(rl) or "ambiguous" in rl or "unclear" in rl:
        return "missing_instruction"
    return "other"


def _metadata_asset_tokens(metadata_snapshot: dict) -> set[str]:
    """Return a lowercased token set of every known asset in the snapshot.

    The set is used by the S3 blame-set rescue: when ASI reports
    ``failure_type == "other"`` but produces a ``blame_set`` containing
    a token that matches a known table, metric view, TVF/UC function, or
    example-SQL identifier, the cascade re-labels the root cause as
    ``missing_data_asset`` (routed to Lever 3) instead of leaving the
    failure at ``other`` (which falls through to generic descriptions).

    Each identifier is also split on ``.`` so bare table names (``orders``
    extracted from ``cat.sch.orders``) can match a blame token.
    """
    tokens: set[str] = set()
    if not isinstance(metadata_snapshot, dict):
        return tokens
    ds = metadata_snapshot.get("data_sources") or {}
    if not isinstance(ds, dict):
        ds = {}
    for bucket_key in ("tables", "metric_views"):
        for asset in metadata_snapshot.get(bucket_key) or ds.get(bucket_key) or []:
            if not isinstance(asset, dict):
                continue
            ident = asset.get("identifier") or asset.get("name") or ""
            if isinstance(ident, str) and ident:
                tokens.add(ident.lower())
                for part in ident.split("."):
                    if part:
                        tokens.add(part.lower())
    instr = metadata_snapshot.get("instructions") or {}
    if not isinstance(instr, dict):
        instr = {}
    for fn in instr.get("sql_functions") or []:
        if not isinstance(fn, dict):
            continue
        name = fn.get("name") or fn.get("identifier") or fn.get("id")
        if isinstance(name, str) and name:
            tokens.add(name.lower())
    for eqs in instr.get("example_question_sqls") or []:
        if not isinstance(eqs, dict):
            continue
        ident = eqs.get("id") or eqs.get("identifier")
        if isinstance(ident, str) and ident:
            tokens.add(ident.lower())
    return tokens


def _blame_set_matches_metadata(
    blame_set: object, metadata_snapshot: dict
) -> bool:
    r"""True when at least one blame token resolves to a known asset.

    Tokens are lowercased and stripped of backticks and trailing
    punctuation so ``\`cat.sch.orders\`,`` matches ``cat.sch.orders``.
    """
    if not blame_set:
        return False
    tokens = _metadata_asset_tokens(metadata_snapshot)
    if not tokens:
        return False
    items = blame_set if isinstance(blame_set, list) else [str(blame_set)]
    for item in items:
        raw = str(item).strip().lower().strip("`").strip(",;")
        if not raw:
            continue
        if raw in tokens:
            return True
        for part in raw.split("."):
            if part and part in tokens:
                return True
    return False


_SQL_KW = re.compile(r"\b(FROM|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|CROSS\s+JOIN|FULL\s+JOIN)\s+", re.I)
_SQL_WHERE = re.compile(r"\bWHERE\b", re.I)
_SQL_GROUP = re.compile(r"\bGROUP\s+BY\b", re.I)
_SQL_MEASURE = re.compile(r"\bMEASURE\s*\(", re.I)
_SQL_TVF = re.compile(r"\b(\w+)\s*\((?:[^)]*,){2,}", re.I)
_SQL_AGG = re.compile(r"\b(SUM|AVG|COUNT|MIN|MAX|STDDEV|VARIANCE)\s*\(", re.I)
_SQL_SELECT_STAR = re.compile(r"\bSELECT\s+\*\b", re.I)
_SQL_SCD_FILTER = re.compile(r"\b(is_current|is_active)\s*=\s*(true|1|'true')\b", re.I)
_SQL_JOIN_TYPE = re.compile(r"\b(LEFT|RIGHT|INNER|CROSS|FULL)\s+(OUTER\s+)?JOIN\b", re.I)
_SQL_WHERE_CONDITIONS = re.compile(r"\bWHERE\b\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|\bHAVING\b|;|\Z)", re.I | re.S)


def _extract_sql_tables(sql: str) -> set[str]:
    """Extract table-like references after FROM/JOIN keywords."""
    tables: set[str] = set()
    for m in _SQL_KW.finditer(sql or ""):
        rest = sql[m.end():m.end() + 200].strip()
        token = rest.split()[0] if rest.split() else ""
        token = token.rstrip(",;)")
        if token and not token.upper().startswith("("):
            tables.add(token.lower())
    return tables


_SQL_JOIN_ON = re.compile(
    r"\bJOIN\s+([\w.`]+)\s+(?:AS\s+)?(\w+)?\s*ON\s+(.+?)(?=\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|;|\Z)",
    re.I | re.S,
)


def _extract_join_pairs(sql: str) -> set[tuple[str, str]]:
    """Extract (table, join_column) pairs from JOIN...ON clauses.

    Parses each ``JOIN <table> ... ON <condition>`` and extracts every
    ``alias.column`` reference from the ON clause, pairing the joined
    table with the column names used in the condition.
    """
    pairs: set[tuple[str, str]] = set()
    for m in _SQL_JOIN_ON.finditer(sql or ""):
        table = m.group(1).strip("`").lower()
        on_clause = m.group(3)
        cols = re.findall(r"[\w`]+\.`?([\w]+)`?", on_clause)
        for col in cols:
            pairs.add((table, col.lower()))
    return pairs


def _extract_instruction_default_filters(
    metadata_snapshot: dict,
) -> list[dict]:
    """Parse Genie Space instructions for default filter rules.

    Returns a list of ``{column, value, pattern}`` dicts representing
    filters that the instructions mandate by default.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions

    instructions = _get_general_instructions(metadata_snapshot)
    if not instructions:
        return []

    filters: list[dict] = []
    lines = instructions.split("\n")
    _FILTER_PATTERNS = [
        re.compile(r"(?:always|by default|default(?:s)? to)\s+(?:filter|use|apply|set)\s+.*?(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
        re.compile(r"(\w+)\s*=\s*['\"]?(\w+)['\"]?\s+(?:by default|unless|is the default)", re.IGNORECASE),
        re.compile(r"(?:unless\s+(?:explicitly|specifically)\s+(?:asked|requested|stated)\s+otherwise).*?(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
    ]

    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue
        for pattern in _FILTER_PATTERNS:
            match = pattern.search(line_stripped)
            if match:
                filters.append({
                    "column": match.group(1).lower(),
                    "value": match.group(2),
                    "pattern": line_stripped[:200],
                })

    instr = metadata_snapshot.get("instructions", {})
    sql_snippets = instr.get("sql_snippets", {}) if isinstance(instr, dict) else {}
    for f_item in sql_snippets.get("filters", []):
        sql_raw = f_item.get("sql", "")
        sql = "".join(str(s) for s in sql_raw).strip() if isinstance(sql_raw, list) else str(sql_raw).strip()
        eq_match = re.match(r"(\w+)\s*=\s*['\"]?(\w+)", sql)
        if eq_match:
            filters.append({
                "column": eq_match.group(1).lower(),
                "value": eq_match.group(2),
                "pattern": f"sql_snippet: {sql}",
            })

    return filters


def _detect_instruction_contradictions(
    original_sections: dict[str, list[str]],
    proposed_sections: dict[str, str | list[str]],
) -> list[dict]:
    """Detect contradictions between user-authored and optimizer-proposed instruction sections.

    Compares filter polarity (always/default vs never/only-when-explicit) for
    the same column across original and proposed sections.  Returns a list of
    contradiction dicts with ``section``, ``original_rule``, ``proposed_line``,
    and ``contradiction_type``.

    Only flags clear inversions to avoid false positives on nuanced rewording.
    """
    _ALWAYS_PATTERNS = [
        re.compile(r"(?:always|by default|default(?:s)?\s+(?:to|filter))\s+.*?(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
        re.compile(r"default\s+filter[:\s]+(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
    ]
    _NEVER_PATTERNS = [
        re.compile(r"(?:never|do\s+not|don'?t)\s+(?:apply|add|filter|use|include)\s+.*?(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
        re.compile(r"only\s+(?:apply|add|filter|use)\s+.*?(\w+)\s*=\s*['\"]?(\w+)['\"]?\s+when\s+.*?(?:explicitly|specifically)", re.IGNORECASE),
        re.compile(r"(\w+)\s*=\s*['\"]?(\w+)['\"]?\s+only\s+when\s+.*?(?:explicitly|specifically)", re.IGNORECASE),
        re.compile(r"absolutely\s+never\s+(?:apply|add|filter|use).*?(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
    ]

    def _extract_filter_rules(sections: dict, patterns: list, polarity: str) -> list[dict]:
        rules: list[dict] = []
        for section_name, lines in sections.items():
            if isinstance(lines, str):
                lines = [lines]
            for line in lines:
                if not isinstance(line, str):
                    continue
                for pat in patterns:
                    match = pat.search(line)
                    if match:
                        rules.append({
                            "column": match.group(1).lower(),
                            "value": match.group(2).lower(),
                            "polarity": polarity,
                            "section": section_name,
                            "line": line.strip(),
                        })
        return rules

    original_always = _extract_filter_rules(original_sections, _ALWAYS_PATTERNS, "always")
    proposed_never = _extract_filter_rules(
        {k: v if isinstance(v, list) else [v] for k, v in proposed_sections.items()},
        _NEVER_PATTERNS, "never",
    )
    original_never = _extract_filter_rules(original_sections, _NEVER_PATTERNS, "never")
    proposed_always = _extract_filter_rules(
        {k: v if isinstance(v, list) else [v] for k, v in proposed_sections.items()},
        _ALWAYS_PATTERNS, "always",
    )

    contradictions: list[dict] = []

    for orig in original_always:
        for prop in proposed_never:
            if orig["column"] == prop["column"]:
                contradictions.append({
                    "section": prop["section"],
                    "original_rule": orig["line"],
                    "proposed_line": prop["line"],
                    "contradiction_type": "filter_inversion",
                    "detail": f"Original says always/default {orig['column']}={orig['value']}, "
                              f"proposed says never/only-explicit",
                })

    for orig in original_never:
        for prop in proposed_always:
            if orig["column"] == prop["column"]:
                contradictions.append({
                    "section": prop["section"],
                    "original_rule": orig["line"],
                    "proposed_line": prop["line"],
                    "contradiction_type": "filter_inversion",
                    "detail": f"Original says never {orig['column']}={orig['value']}, "
                              f"proposed says always/default",
                })

    return contradictions


def _classify_generated_sql_quality(generated_sql: str, question: str = "") -> str:
    """Classify structural issues in generated SQL when expected SQL is missing.

    Provides more actionable root causes than ``"other"`` by analyzing the
    SQL's structure against the question text.
    """
    gen_lower = generated_sql.lower()
    q_lower = question.lower() if question else ""

    if re.search(r"\bcount\s*\(\s*\*\s*\)", gen_lower) and not re.search(r"\b(?:count|how many)\b", q_lower):
        return "exploratory_query"

    gen_has_group = bool(re.search(r"\bgroup\s+by\b", gen_lower))
    asks_for_by = bool(re.search(r"\bby\s+\w+", q_lower))
    if asks_for_by and not gen_has_group:
        return "missing_aggregation"

    gen_has_where = bool(re.search(r"\bwhere\b", gen_lower))
    filter_keywords = re.findall(r"\b(?:for|only|just|specific|in|from)\s+(\w+)", q_lower)
    if filter_keywords and not gen_has_where:
        return "missing_filter"

    gen_selects = re.findall(r"\bselect\b(.+?)\bfrom\b", gen_lower, re.S)
    if gen_selects:
        gen_cols = [c.strip() for c in gen_selects[0].split(",")]
        if len(gen_cols) <= 2 and asks_for_by:
            return "wrong_granularity"

    return "unverifiable_no_expected_sql"


# ──────────────────────────────────────────────────────────────────────
# T1.2 — Pattern-aware root cause detectors
#
# The legacy heuristic taxonomy (wrong_table / missing_aggregation /
# wrong_filter_condition / wrong_aggregation) fragments a single
# underlying pattern into four clusters (e.g. time_window pivoting shows
# up as wrong_filter_condition on one row, missing_aggregation on another,
# wrong_table on a third). These pure-function matchers detect the actual
# *pattern* from the (genie_sql, gt_sql) pair and return a specific
# pattern_label. When a matcher fires with sufficient confidence, it
# takes precedence over the legacy bucket; otherwise we fall through to
# ``_classify_sql_diff``.
# ──────────────────────────────────────────────────────────────────────


_TIME_WINDOW_WHERE_RE = re.compile(
    r"where[^;]*?\btime_window\s+in\s*\(",
    re.IGNORECASE | re.DOTALL,
)
_TIME_WINDOW_GROUPBY_RE = re.compile(
    r"group\s+by[^;]*?\btime_window\b",
    re.IGNORECASE | re.DOTALL,
)
_RANK_EQ_1_RE = re.compile(
    r"\brank\s*\([^)]*\)\s*(?:over\s*\([^)]*\))?"
    r"[^;]*?\s*=\s*1\b",
    re.IGNORECASE | re.DOTALL,
)
_LIMIT_1_RE = re.compile(r"\blimit\s+1\b(?!\d)", re.IGNORECASE)
_LIMIT_N_RE = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)
_PLURAL_TOP_N_QUESTION_RE = re.compile(
    r"\b(which|what)\s+[a-z]+\s+(have|has)\s+(the\s+)?(highest|most|top|"
    r"largest|smallest|lowest)\b",
    re.IGNORECASE,
)
_STRING_LITERAL_RE = re.compile(r"'([^']{1,64})'")

# P1 — column_disambiguation / granularity_drop helpers.
# Identifier extractor for SELECT/WHERE/GROUP-BY tokens; deliberately
# permissive so we catch ``schema.column``, ``alias.column``, and bare
# names. Aliases (``AS something``) are stripped before matching.
_IDENTIFIER_RE = re.compile(
    r"(?:[A-Za-z_][A-Za-z0-9_]*\.)*([A-Za-z_][A-Za-z0-9_]*)"
)
_GROUP_BY_BLOCK_RE = re.compile(
    r"\bgroup\s+by\b(.+?)(?=\b(order\s+by|having|limit|window|qualify|"
    r"union|except|intersect|;|$))",
    re.IGNORECASE | re.DOTALL,
)
_SELECT_BLOCK_RE = re.compile(
    r"\bselect\b(.+?)\bfrom\b",
    re.IGNORECASE | re.DOTALL,
)


def _extract_columns_from_block(block: str) -> set[str]:
    """Return lowercase identifiers referenced in *block* (SELECT/GROUP BY).

    Strips aliases (``AS x``), measure / function calls (we keep their
    inner identifiers), and string literals before tokenising. Used by
    the granularity_drop and column_disambiguation matchers.
    """
    if not block:
        return set()
    s = re.sub(r"'[^']*'", " ", block)
    s = re.sub(r"\s+as\s+[A-Za-z_][A-Za-z0-9_]*", " ", s, flags=re.IGNORECASE)
    out: set[str] = set()
    for m in _IDENTIFIER_RE.finditer(s):
        tok = m.group(1).lower()
        if tok in {
            "select", "from", "where", "and", "or", "not", "group",
            "by", "order", "limit", "asc", "desc", "case", "when",
            "then", "else", "end", "as", "on", "using", "join",
            "inner", "left", "right", "outer", "full", "is", "null",
            "in", "exists", "between", "like", "ilike", "true", "false",
            "all", "any", "some", "distinct", "having", "with", "over",
            "partition", "rows", "range", "preceding", "following",
            "current", "row", "interval",
        }:
            continue
        # Skip pure SQL functions / aggregate names. We still keep
        # their argument identifiers (the regex returns the *last*
        # capture per match, so ``MEASURE(foo)`` already yields ``foo``).
        if tok in {"measure", "sum", "count", "avg", "min", "max",
                   "rank", "dense_rank", "row_number", "coalesce",
                   "nullif", "cast", "extract", "date_trunc", "date_add",
                   "year", "month", "day", "to_date", "current_date"}:
            continue
        out.add(tok)
    return out


def _common_prefix_len(a: str, b: str) -> int:
    """Return length of the shared lowercase prefix of *a* and *b*."""
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i].lower() == b[i].lower():
        i += 1
    return i


# ── Column-confusion analyzer (regression-mining loop) ───────────────
#
# The legacy ``column_disambiguation`` detector inside
# ``_detect_failure_pattern`` keys off a >=5-char shared prefix and
# only inspects projected columns. That misses real-world abbreviation
# pairs (``is_month_to_date`` vs ``use_mtdate_flag`` — tokens
# ``[month, to, date]`` collapse into a single subtoken ``mtdate``)
# and swaps inside ``WHERE`` / ``GROUP BY`` / ``MEASURE(...)``.
#
# ``detect_column_confusion`` is a pure helper used by the regression-
# mining lane: it returns structured ``ColumnConfusion`` evidence
# without classifying the failure or routing it to a lever. Acceptance
# semantics are unchanged; mining consumes these insights only after
# the candidate has already been rolled back.

_WHERE_BLOCK_RE = re.compile(
    r"\bwhere\b(.+?)(?=\bgroup\s+by\b|\border\s+by\b|\bhaving\b|"
    r"\blimit\b|\bwindow\b|\bqualify\b|\bunion\b|\bexcept\b|"
    r"\bintersect\b|;|$)",
    re.IGNORECASE | re.DOTALL,
)
_MEASURE_CALL_RE = re.compile(
    r"measure\s*\(\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\s*\)",
    re.IGNORECASE,
)
_FROM_TABLE_RE = re.compile(
    r"\bfrom\s+([A-Za-z_][A-Za-z0-9_\.]*)",
    re.IGNORECASE,
)
_BACKTICK_IDENT_RE = re.compile(r"`([^`]+)`")


@dataclass(frozen=True)
class ColumnConfusion:
    """Evidence that a generated SQL substituted a similar-looking column.

    ``intended_column`` is the column referenced by the expected SQL
    that does not appear in the generated SQL. ``confused_column`` is
    the column the generated SQL used in its place. ``sql_clause`` is
    one of ``select`` / ``where`` / ``group_by`` / ``measure``.
    ``confidence`` is a coarse 0-1 score (0.85 for shared-prefix +
    metadata corroboration, 0.7 for prefix-only or token overlap).
    ``rationale`` is a short human-readable explanation.
    """

    intended_column: str
    confused_column: str
    table: str | None
    sql_clause: str
    confidence: float
    rationale: str


_MIN_ABBREV_LEN = 3


def _abbrev_candidates(name: str) -> set[str]:
    """Generate normalized abbreviation forms for a column name.

    For each contiguous subsequence of underscore tokens, emits:

    * the full concatenation (``month_to_date`` -> ``monthtodate``);
    * the pure initials (``month_to_date`` -> ``mtd``);
    * the "abbreviate prefix, keep suffix whole" forms — e.g. with
      one whole tail token, ``month_to_date`` -> ``mtdate``;
    * the "keep prefix whole, abbreviate suffix" forms — e.g.
      ``month_to_date`` -> ``monthtd``.

    All candidates shorter than :data:`_MIN_ABBREV_LEN` are dropped so
    common prepositional tokens (``is``, ``to``, ``in``) cannot
    spuriously match across unrelated columns. Used by
    :func:`_columns_overlap_by_token` to find pairs like
    ``is_month_to_date`` vs ``use_mtdate_flag`` where the legacy
    shared-prefix detector returns no match.
    """
    if not name:
        return set()
    parts = [p for p in re.split(r"[_\W]+", str(name).lower()) if p]
    if not parts:
        return set()
    out: set[str] = set()
    # Full standalone tokens (>= min length) so single-token columns
    # match against any subsequence containing them.
    for p in parts:
        if len(p) >= _MIN_ABBREV_LEN:
            out.add(p)
    n = len(parts)
    for i in range(n):
        for j in range(i + 2, n + 1):
            sub = parts[i:j]
            full = "".join(sub)
            if len(full) >= _MIN_ABBREV_LEN:
                out.add(full)
            initials = "".join(p[0] for p in sub if p)
            if len(initials) >= _MIN_ABBREV_LEN:
                out.add(initials)
            for k in range(1, len(sub)):
                # Abbreviate prefix to initials, keep last k tokens whole.
                cand = (
                    "".join(p[0] for p in sub[:-k] if p)
                    + "".join(sub[-k:])
                )
                if len(cand) >= _MIN_ABBREV_LEN:
                    out.add(cand)
                # Keep first k tokens whole, abbreviate suffix.
                cand = (
                    "".join(sub[:k])
                    + "".join(p[0] for p in sub[k:] if p)
                )
                if len(cand) >= _MIN_ABBREV_LEN:
                    out.add(cand)
    return out


def _columns_overlap_by_token(a: str, b: str) -> tuple[bool, str]:
    """Return ``(matches, rationale)`` for token/abbrev overlap of two
    column names.

    The match is symmetric and uses :func:`_abbrev_candidates` on both
    sides, so e.g. ``mtdate`` (B's token) can match against the
    abbreviation ``mtdate`` derived from A's ``[month, to, date]``
    subsequence. The rationale is a short string explaining why the
    columns matched so callers can persist it without re-deriving.
    """
    if not a or not b or a == b:
        return False, ""
    cand_a = _abbrev_candidates(a)
    cand_b = _abbrev_candidates(b)
    if not cand_a or not cand_b:
        return False, ""
    shared = cand_a & cand_b
    if shared:
        return True, f"shared/abbreviated subtokens: {sorted(shared)[:5]}"
    return False, ""


def _columns_for_clause(sql: str, clause: str) -> set[str]:
    """Return columns referenced under *clause* in *sql*.

    *clause* is one of ``select`` / ``where`` / ``group_by`` /
    ``measure``. Returns lowercase identifiers; deliberately permissive
    on syntax to keep parity with the existing ``_detect_failure_pattern``
    helpers (regex over sqlglot for unparseable SQL).
    """
    if not sql:
        return set()
    if clause == "select":
        m = _SELECT_BLOCK_RE.search(sql)
        return _extract_columns_from_block(m.group(1)) if m else set()
    if clause == "where":
        m = _WHERE_BLOCK_RE.search(sql)
        return _extract_columns_from_block(m.group(1)) if m else set()
    if clause == "group_by":
        m = _GROUP_BY_BLOCK_RE.search(sql)
        return _extract_columns_from_block(m.group(1)) if m else set()
    if clause == "measure":
        return {m.group(1).lower() for m in _MEASURE_CALL_RE.finditer(sql)}
    return set()


def _extract_table_hint(sql: str) -> str | None:
    """Best-effort table extraction from the first FROM clause."""
    if not sql:
        return None
    m = _FROM_TABLE_RE.search(sql)
    if not m:
        return None
    raw = m.group(1).strip().rstrip(";").rstrip(",")
    return raw or None


def detect_column_confusion(
    expected_sql: str,
    generated_sql: str,
    *,
    metadata_snapshot: dict | None = None,
    min_prefix_len: int = 5,
) -> list[ColumnConfusion]:
    """Return structured column-confusion evidence between two SQLs.

    Compares expected vs generated SQL across the SELECT, WHERE,
    GROUP BY, and MEASURE(...) surfaces. For each clause, finds
    expected-only columns and generated-only columns, then pairs them
    when one of the following matches:

    * shared lowercase prefix >= ``min_prefix_len`` (legacy disambig
      pattern), OR
    * shared subtokens of length >= 3, OR
    * initial-letter abbreviation of one side appears as a substring
      of the other side.

    *metadata_snapshot* is consulted only to bump confidence when both
    columns belong to the same table with the same data type. The
    function does not require it; absent metadata the result is still
    valid (lower confidence).

    Pure: no I/O, no LLM, no Genie SDK. Safe to call from anywhere in
    the optimizer.
    """
    if not (expected_sql or "").strip() or not (generated_sql or "").strip():
        return []

    table_hint = (
        _extract_table_hint(expected_sql)
        or _extract_table_hint(generated_sql)
    )

    insights: list[ColumnConfusion] = []
    seen: set[tuple[str, str, str]] = set()

    for clause in ("select", "where", "group_by", "measure"):
        exp_cols = _columns_for_clause(expected_sql, clause)
        gen_cols = _columns_for_clause(generated_sql, clause)
        exp_only = exp_cols - gen_cols
        gen_only = gen_cols - exp_cols
        if not exp_only or not gen_only:
            continue
        for exp_c in sorted(exp_only):
            best: tuple[ColumnConfusion, float] | None = None
            for gen_c in sorted(gen_only):
                if exp_c == gen_c:
                    continue
                prefix = _common_prefix_len(exp_c, gen_c)
                token_match, token_reason = _columns_overlap_by_token(
                    exp_c, gen_c,
                )
                if prefix < min_prefix_len and not token_match:
                    continue
                # Confidence: prefix dominates (cheaper, well-tested).
                # Token-only matches start lower and bump on metadata.
                if prefix >= min_prefix_len:
                    conf = 0.7
                    rationale = (
                        f"columns share {prefix}-char prefix"
                    )
                else:
                    conf = 0.6
                    rationale = token_reason or "subtoken overlap"
                same_type = _same_data_type(
                    metadata_snapshot, exp_c, gen_c,
                )
                if same_type:
                    conf = min(0.9, conf + 0.15)
                    rationale = f"{rationale}; metadata confirms same data type"
                key = (exp_c, gen_c, clause)
                if key in seen:
                    continue
                seen.add(key)
                cand = ColumnConfusion(
                    intended_column=exp_c,
                    confused_column=gen_c,
                    table=table_hint,
                    sql_clause=clause,
                    confidence=conf,
                    rationale=(
                        f"GT {clause} uses {exp_c!r}; "
                        f"Genie {clause} uses {gen_c!r}; {rationale}."
                    ),
                )
                if best is None or cand.confidence > best[1]:
                    best = (cand, cand.confidence)
            if best is not None:
                insights.append(best[0])

    return insights


def _same_data_type(
    metadata_snapshot: dict | None,
    col_a: str,
    col_b: str,
) -> bool:
    """Return True iff *col_a* and *col_b* live on the same table with
    the same data type, per *metadata_snapshot*. Defensive: any shape
    mismatch returns False."""
    if not isinstance(metadata_snapshot, dict):
        return False
    tables = metadata_snapshot.get("tables")
    if not isinstance(tables, dict):
        return False
    a = (col_a or "").lower()
    b = (col_b or "").lower()
    for _t_meta in tables.values():
        cols = _t_meta.get("columns") if isinstance(_t_meta, dict) else None
        if not isinstance(cols, dict):
            continue
        if a not in cols or b not in cols:
            continue
        ta = cols.get(a) or {}
        tb = cols.get(b) or {}
        if not isinstance(ta, dict) or not isinstance(tb, dict):
            continue
        ta_t = str(ta.get("data_type", "")).lower()
        tb_t = str(tb.get("data_type", "")).lower()
        if ta_t and tb_t and ta_t == tb_t:
            return True
    return False


def _detect_failure_pattern(ctx: dict) -> tuple[str | None, float, str]:
    """T1.2: Return (pattern_label, confidence, evidence) for a failure.

    Runs a small library of structural pattern matchers on
    ``(expected_sql, generated_sql, question)``. Returns ``(None, 0.0,
    "")`` when no pattern fires — callers then fall back to the legacy
    ``_classify_sql_diff`` heuristic.

    Pattern labels (stable, used downstream for cluster keying):

    - ``time_window_pivot``:  Genie pivots ``time_window`` as a row
      dimension via ``WHERE time_window IN (...)`` + GROUP BY, while GT
      uses measure-suffix-on-same-row. Drives Q1 / Q10 / Q11 / Q17 in
      the retail corpus.
    - ``plural_top_n_collapse``:  the question asks "which stores have
      the highest ..." (plural) but Genie returns top-1 via ``RANK()=1``
      or ``LIMIT 1`` while GT returns an ordered list (or LIMIT N > 1).
      Drives Q2 / Q6.
    - ``value_format_mismatch``:  Genie's WHERE-clause literal doesn't
      match any literal format GT used on the same column (e.g.
      ``state_name = 'Virginia'`` vs GT ``state_name = 'VA'``). Drives
      Q19.
    - ``column_disambiguation``:  the chosen column shares a prefix with
      at least one other column that has the same data type — a
      disambiguation pair. Drives Q3 (is_one_day_prior_year_same_day
      vs is_month_to_date_prior_year_same_day).
    - ``granularity_drop``:  GT groups by ``{K1, K2, ...}``; Genie's
      GROUP BY is a strict subset; the missing keys appear in GT's
      SELECT projection (i.e. they're carried as output dimensions,
      not just join keys). Drives Q11 (GT groups by time_window and
      returns per-time-window rows; Genie drops time_window).

    Each detector is independent and cheap; they run in declared order
    and the first with confidence >= 0.7 wins. Confidence is a coarse
    indicator (0.7 for structural patterns, 0.9 when the question text
    independently corroborates).
    """
    expected_sql = (ctx.get("expected_sql") or "").strip()
    generated_sql = (ctx.get("generated_sql") or "").strip()
    question = (ctx.get("question") or "").strip()

    if not expected_sql and not generated_sql:
        return None, 0.0, ""

    exp_lower = expected_sql.lower()
    gen_lower = generated_sql.lower()

    # 1. time_window_pivot — Genie adds a pivot dimension GT did not.
    if (
        generated_sql
        and _TIME_WINDOW_WHERE_RE.search(gen_lower)
        and _TIME_WINDOW_GROUPBY_RE.search(gen_lower)
        and not _TIME_WINDOW_GROUPBY_RE.search(exp_lower)
    ):
        return (
            "time_window_pivot",
            0.9,
            "Genie has WHERE time_window IN (...) + GROUP BY time_window; "
            "GT does not group by time_window.",
        )

    # 2. plural_top_n_collapse — Genie collapses plural question to top-1.
    _question_is_plural = bool(_PLURAL_TOP_N_QUESTION_RE.search(question))
    _gen_is_top1 = bool(
        _RANK_EQ_1_RE.search(gen_lower) or _LIMIT_1_RE.search(gen_lower)
    )
    _exp_is_top1 = bool(
        _RANK_EQ_1_RE.search(exp_lower) or _LIMIT_1_RE.search(exp_lower)
    )
    _exp_limit_match = _LIMIT_N_RE.search(exp_lower)
    _exp_limit_n = int(_exp_limit_match.group(1)) if _exp_limit_match else None
    _exp_is_ordered_list = (
        not _exp_is_top1
        and (
            (_exp_limit_n is not None and _exp_limit_n > 1)
            or "order by" in exp_lower
        )
    )
    if _gen_is_top1 and _exp_is_ordered_list:
        _conf = 0.9 if _question_is_plural else 0.7
        return (
            "plural_top_n_collapse",
            _conf,
            (
                f"Genie returns top-1 via RANK=1/LIMIT 1; "
                f"GT returns ordered list"
                + (f" (LIMIT {_exp_limit_n})" if _exp_limit_n else "")
                + (
                    "; question phrased as plural ('which ... have the "
                    "highest')"
                    if _question_is_plural
                    else ""
                )
            ),
        )

    # 3. value_format_mismatch — WHERE literal on same column differs in
    #    format. We compare the set of single-quoted literals in each
    #    SQL; a mismatch where GT literal is a SHORT form (<=5 chars)
    #    and Genie literal is a LONG form (>=6 chars) starting with the
    #    same letter is the canonical "full-name vs code" pattern.
    _exp_lits = set(_STRING_LITERAL_RE.findall(expected_sql))
    _gen_lits = set(_STRING_LITERAL_RE.findall(generated_sql))
    if _exp_lits and _gen_lits and _exp_lits != _gen_lits:
        for _gl in _gen_lits:
            if _gl in _exp_lits:
                continue
            for _el in _exp_lits:
                if _el in _gen_lits:
                    continue
                if (
                    len(_el) <= 5 and len(_gl) >= 6
                    and _el[:1].lower() == _gl[:1].lower()
                ):
                    return (
                        "value_format_mismatch",
                        0.85,
                        (
                            f"Genie literal {_gl!r} differs from GT "
                            f"literal {_el!r}; GT uses short form "
                            f"({len(_el)} chars), Genie uses long form "
                            f"({len(_gl)} chars)."
                        ),
                    )

    # P1 — column_disambiguation. The chosen column shares a common
    # prefix (>= 5 chars) with at least one other column on the same
    # table, and the table metadata (when available) confirms both
    # share a data type. We compare GT's projected/filtered columns
    # vs Genie's; when the unique GT-only column shares a long prefix
    # with the unique Genie-only column on the same table, the pattern
    # fires. Confidence 0.85 if metadata corroborates same-type, else
    # 0.7 (prefix-only).
    _gt_select_match = _SELECT_BLOCK_RE.search(expected_sql)
    _gn_select_match = _SELECT_BLOCK_RE.search(generated_sql)
    _gt_select_cols = (
        _extract_columns_from_block(_gt_select_match.group(1))
        if _gt_select_match else set()
    )
    _gn_select_cols = (
        _extract_columns_from_block(_gn_select_match.group(1))
        if _gn_select_match else set()
    )
    _gt_only = _gt_select_cols - _gn_select_cols
    _gn_only = _gn_select_cols - _gt_select_cols
    _metadata_snapshot = ctx.get("metadata_snapshot") or {}
    if _gt_only and _gn_only:
        for _gtc in _gt_only:
            for _gnc in _gn_only:
                if _gtc == _gnc:
                    continue
                _prefix = _common_prefix_len(_gtc, _gnc)
                if _prefix < 5:
                    continue
                # Try to corroborate via metadata: are both columns
                # present in any one table with the same data type?
                _same_type = False
                _tables = (
                    _metadata_snapshot.get("tables")
                    if isinstance(_metadata_snapshot, dict) else None
                )
                if isinstance(_tables, dict):
                    for _t_name, _t_meta in _tables.items():
                        _cols = (
                            _t_meta.get("columns")
                            if isinstance(_t_meta, dict) else None
                        )
                        if not isinstance(_cols, dict):
                            continue
                        if _gtc not in _cols or _gnc not in _cols:
                            continue
                        _t1 = (
                            (_cols.get(_gtc) or {}).get("data_type", "")
                            if isinstance(_cols.get(_gtc), dict) else ""
                        )
                        _t2 = (
                            (_cols.get(_gnc) or {}).get("data_type", "")
                            if isinstance(_cols.get(_gnc), dict) else ""
                        )
                        if _t1 and _t2 and str(_t1).lower() == str(_t2).lower():
                            _same_type = True
                            break
                _conf = 0.85 if _same_type else 0.7
                return (
                    "column_disambiguation",
                    _conf,
                    (
                        f"GT projects {_gtc!r}; Genie projects "
                        f"{_gnc!r}; columns share {_prefix}-char prefix"
                        + (
                            "; metadata confirms same data type."
                            if _same_type else "."
                        )
                    ),
                )

    # P1 — granularity_drop. GT groups by ``{K1, K2, ...}``; Genie's
    # GROUP BY is a strict subset; the missing keys appear in GT's
    # SELECT projection (carried as output dimensions). Confidence
    # 0.85.
    _gt_group_match = _GROUP_BY_BLOCK_RE.search(expected_sql)
    _gn_group_match = _GROUP_BY_BLOCK_RE.search(generated_sql)
    if _gt_group_match and _gn_group_match:
        _gt_keys = _extract_columns_from_block(_gt_group_match.group(1))
        _gn_keys = _extract_columns_from_block(_gn_group_match.group(1))
        _missing = _gt_keys - _gn_keys
        if _gt_keys and _gn_keys and _missing and _gn_keys < _gt_keys:
            _carried = _missing & _gt_select_cols
            if _carried:
                return (
                    "granularity_drop",
                    0.85,
                    (
                        f"GT GROUP BY includes {sorted(_gt_keys)}; "
                        f"Genie GROUP BY drops {sorted(_missing)}; "
                        f"dropped keys appear in GT projection: "
                        f"{sorted(_carried)}."
                    ),
                )

    # No pattern matched with sufficient confidence.
    return None, 0.0, ""


def _classify_sql_diff(ctx: dict) -> str:
    """Classify a failure's root cause by comparing expected vs generated SQL.

    Accepts either a ``sql_context`` dict (with ``expected_sql`` / ``generated_sql``
    keys) or a full row dict (with ``request`` / ``response`` keys).
    Falls back to ``"other"`` when the SQL pair is missing or no pattern matches.

    T1.2: delegates to ``_detect_failure_pattern`` first; when a pattern
    matcher fires with confidence >= 0.7, its label is returned instead
    of the legacy bucket. This keeps the existing callers unchanged
    while opportunistically surfacing more specific pattern labels when
    they're available.
    """
    _pattern_label, _pattern_conf, _pattern_evidence = _detect_failure_pattern(ctx)
    if _pattern_label and _pattern_conf >= 0.7:
        # Stash the evidence on the ctx so downstream cluster keying /
        # logs can reference it. The return value stays a bare string
        # for back-compat with legacy callers.
        if isinstance(ctx, dict):
            ctx.setdefault("_pattern_label", _pattern_label)
            ctx.setdefault("_pattern_confidence", _pattern_conf)
            ctx.setdefault("_pattern_evidence", _pattern_evidence)
        return _pattern_label
    expected_sql = (ctx.get("expected_sql") or "").strip()
    generated_sql = (ctx.get("generated_sql") or "").strip()

    if not expected_sql:
        req = ctx.get("request") or {}
        if isinstance(req, str):
            try:
                req = json.loads(req)
            except (json.JSONDecodeError, TypeError):
                req = {}
        expected_sql = (req.get("expected_sql") or "").strip()
    if not generated_sql:
        resp = ctx.get("response") or {}
        if isinstance(resp, str):
            try:
                resp = json.loads(resp)
            except (json.JSONDecodeError, TypeError):
                resp = {}
        generated_sql = (resp.get("response") or "").strip()

    if not expected_sql or not generated_sql:
        if generated_sql:
            question = (ctx.get("request") or {}).get("question", "") if isinstance(ctx.get("request"), dict) else ""
            return _classify_generated_sql_quality(generated_sql, question)
        return "other"

    exp_lower = expected_sql.lower()
    gen_lower = generated_sql.lower()

    exp_tables = _extract_sql_tables(expected_sql)
    gen_tables = _extract_sql_tables(generated_sql)
    exp_join_pairs = _extract_join_pairs(expected_sql)
    gen_join_pairs = _extract_join_pairs(generated_sql)
    exp_has_join = bool(re.search(r"\bJOIN\b", exp_lower))

    _DIM_PREFIXES = ("dim_", "lookup_", "ref_")
    missing_tables = exp_tables - gen_tables if exp_tables and gen_tables else set()

    # 1. Missing dimension JOIN — GT joins a dim/lookup/ref table that
    #    Genie omits entirely. This is fundamentally a join issue even if
    #    aggregation or filter differences also exist.
    if missing_tables:
        dim_tables = {t for t in missing_tables if any(t.startswith(p) or f".{p}" in t for p in _DIM_PREFIXES)}
        if dim_tables:
            return "wrong_join"

    # 2. Aggregation checks
    exp_aggs = set(_SQL_AGG.findall(exp_lower))
    gen_aggs = set(_SQL_AGG.findall(gen_lower))
    if exp_aggs and not gen_aggs:
        return "missing_aggregation"
    if exp_aggs != gen_aggs and exp_aggs and gen_aggs:
        return "wrong_aggregation"

    # 3. Filter checks
    exp_has_where = bool(_SQL_WHERE.search(exp_lower))
    gen_has_where = bool(_SQL_WHERE.search(gen_lower))
    if exp_has_where and not gen_has_where:
        return "missing_filter"

    # 3b. SCD filter — expected has is_current/is_active = true, generated omits it
    exp_has_scd = bool(_SQL_SCD_FILTER.search(exp_lower))
    gen_has_scd = bool(_SQL_SCD_FILTER.search(gen_lower))
    if exp_has_scd and not gen_has_scd:
        return "missing_scd_filter"

    # 3c. WHERE condition diff — both have WHERE but conditions differ
    if exp_has_where and gen_has_where:
        exp_conds = _SQL_WHERE_CONDITIONS.search(exp_lower)
        gen_conds = _SQL_WHERE_CONDITIONS.search(gen_lower)
        if exp_conds and gen_conds:
            exp_text = re.sub(r"\s+", " ", exp_conds.group(1).strip())
            gen_text = re.sub(r"\s+", " ", gen_conds.group(1).strip())
            if exp_text != gen_text:
                return "wrong_filter_condition"

    # 3d. Join type diff — same tables but different join types (LEFT vs INNER)
    exp_join_types = sorted(_SQL_JOIN_TYPE.findall(exp_lower))
    gen_join_types = sorted(_SQL_JOIN_TYPE.findall(gen_lower))
    if exp_join_types and gen_join_types and exp_join_types != gen_join_types:
        return "wrong_join_type"

    # 4. Wrong join column — both queries join the same tables but on
    #    different columns (e.g. destination_name vs destination_key).
    if exp_join_pairs and gen_join_pairs and exp_join_pairs != gen_join_pairs:
        return "wrong_join_spec"

    # 5. Missing join — GT has a JOIN that Genie doesn't (regardless of
    #    whether Genie has other JOINs).
    if exp_has_join and missing_tables:
        return "wrong_join"

    # 6. SELECT *, TVF, MEASURE, GROUP BY checks
    gen_has_star = bool(_SQL_SELECT_STAR.search(gen_lower))
    exp_has_star = bool(_SQL_SELECT_STAR.search(exp_lower))
    if gen_has_star and not exp_has_star:
        return "select_star"

    exp_has_tvf = bool(_SQL_TVF.search(exp_lower))
    gen_has_tvf = bool(_SQL_TVF.search(gen_lower))
    if exp_has_tvf != gen_has_tvf:
        return "tvf_parameter_error"

    exp_has_measure = bool(_SQL_MEASURE.search(exp_lower))
    gen_has_measure = bool(_SQL_MEASURE.search(gen_lower))
    if exp_has_measure != gen_has_measure:
        return "wrong_measure"

    exp_has_group = bool(_SQL_GROUP.search(exp_lower))
    gen_has_group = bool(_SQL_GROUP.search(gen_lower))
    if exp_has_group != gen_has_group:
        return "wrong_aggregation"

    # 7. Table-set differs (non-dimension tables)
    if exp_tables and gen_tables and exp_tables != gen_tables:
        extra_tables = gen_tables - exp_tables
        date_dims = {t for t in missing_tables if "dim_date" in t or "dim_time" in t}
        if missing_tables == date_dims and not extra_tables:
            return "format_difference"
        if missing_tables or extra_tables:
            return "wrong_table"

    # 8. Same tables, different columns
    if exp_tables == gen_tables and exp_tables:
        exp_select = re.findall(r"\bSELECT\b(.+?)\bFROM\b", exp_lower, re.S)
        gen_select = re.findall(r"\bSELECT\b(.+?)\bFROM\b", gen_lower, re.S)
        if exp_select and gen_select:
            exp_cols = [c.strip() for c in exp_select[0].split(",")]
            gen_cols = [c.strip() for c in gen_select[0].split(",")]
            if len(exp_cols) > len(gen_cols) and len(gen_cols) >= 1:
                return "extra_columns_only"
        return "wrong_column"

    return "missing_instruction"


def _normalize_ast_diff_sql_pairs(raw: Any) -> list[dict[str, str]]:
    """Normalize row-level SQL pairs into the dict shape AFS expects.

    ``harness.py`` writes ``(generated_sql, expected_sql)``;
    ``afs._structural_diff`` expects ``{"expected_sql": ..., "generated_sql": ...}``.
    Accept both shapes so future callers can use the clearer dict form.
    """
    if not raw:
        return []
    items = raw if isinstance(raw, list) else [raw]
    out: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            expected_sql = str(item.get("expected_sql") or "")
            generated_sql = str(item.get("generated_sql") or "")
        elif isinstance(item, (tuple, list)) and len(item) >= 2:
            generated_sql = str(item[0] or "")
            expected_sql = str(item[1] or "")
        else:
            continue
        if expected_sql and generated_sql:
            pair = {"expected_sql": expected_sql, "generated_sql": generated_sql}
            if pair not in out:
                out.append(pair)
    return out[:5]


def _summarize_feature_diffs(raw: list[Any]) -> dict[str, Any]:
    """Summarize feature_mining.SqlDiff objects without carrying raw SQL.

    Prompt-facing output is intentionally a closed set of enum-like tokens and
    integer lever IDs. Keep table names, column names, and SQL text out of this
    block; AFS carries structural diffs separately through its leak-safe
    projection path.
    """
    kinds: list[str] = []
    levers: list[int] = []
    for diff in raw:
        primary = getattr(diff, "primary_kind", None)
        kind = getattr(primary, "value", None) or str(primary or "")
        if kind and kind != "None" and kind not in kinds:
            kinds.append(kind)
        for lever in getattr(diff, "candidate_levers", ()) or ():
            if int(lever) not in levers:
                levers.append(int(lever))
    if not kinds:
        return {}
    return {
        "primary_kind": kinds[0],
        "kinds": kinds,
        "candidate_levers": levers,
    }


def cluster_failures(
    eval_results: dict,
    metadata_snapshot: dict,
    *,
    spark: Any = None,
    run_id: str = "",
    catalog: str = "",
    schema: str = "",
    verbose: bool = True,
    held_out_qids: set[str] | None = None,
    qid_state: dict | None = None,
    signal_type: str = "hard",
    namespace: str | None = None,
    # ── Plan 4 — LLM-driven clustering short-circuit (back-compat) ──
    rca_evidence_typed: Any = None,
    prior_clusters: list[dict] | None = None,
    w: Any = None,
) -> list[dict]:
    """Group evaluation failures into actionable clusters.

    Groups by ``(judge, asi_failure_type, blame_set_str)``.  Falls back to
    ``(judge, _extract_pattern(rationale), "")`` when ASI is absent.
    Returns clusters with >= 1 question so even single failures are actionable.

    Tier 2.11: the caller can supply a shared ``qid_state`` dict so hard
    and soft clustering passes see the same ``:vN`` dedup state. Without
    this, the same physical row shows up as ``Q001`` in the hard cluster
    and ``Q001`` in the soft cluster with two different root causes —
    the DO-NOT-RETRY bookkeeping then gets confused. ``signal_type`` is
    stamped on each cluster for downstream consumers.

    T1.9: ``namespace`` controls the cluster_id prefix. Hard passes should
    pass ``namespace="H"`` to mint ``H001, H002, ...``; soft passes should
    pass ``namespace="S"`` to mint ``S001, S002, ...``. Without this, both
    passes collide on the ``C001, C002, ...`` namespace and downstream
    consumers that key off ``source_cluster_ids`` cannot tell whether a
    cluster is hard or soft. Defaults to ``"H"`` when ``signal_type="hard"``
    and ``"S"`` otherwise, so callers that don't specify get sensible
    behaviour. Legacy ``C###`` IDs are still accepted by downstream
    parsers for replay compatibility with old iterations.

    When ``spark/run_id/catalog/schema`` are provided, enriches with stored
    ASI data from ``genie_eval_asi_results`` Delta table.

    Bug #4 (P4.2) — ``held_out_qids`` (optional) is a set of benchmark ids
    reserved for baseline/finalize evaluation. Any row whose question_id is
    in this set is dropped before clustering; downstream LLM prompts
    (which consume the clusters) therefore NEVER see held-out content.
    Callers that pass ``held_out_qids=None`` get the legacy "train+held-
    out mixed" behaviour but SHOULD be updated.
    """
    # ── Plan 11 — LLM-first diagnose + cluster (top priority). ─────────
    # Plan 12 PR 1 split the silent `and rca_evidence_typed`
    # short-circuit into an explicit dispatch helper that emits one
    # GSO_PLAN11_DISPATCH_DECISION_V1 marker per call (outcome
    # "entered" / "skipped" + typed skip_reason). This closes the
    # silent-fallthrough bug both 2026-05-20 postmortems observed and
    # lets Plan 11 run even when Plan 3's deterministic RCA classifier
    # produced empty typed evidence (the raw-eval fallback adapter
    # builds Stage 1 input from failing_qids + eval_rows directly).
    from genie_space_optimizer.common.config import (
        plan4_llm_clustering_enabled,
    )

    # Plan 12 Step 1.3.5 — derive _failing_qids / _eval_rows_failing
    # from eval_results when the iteration scope didn't pre-stamp them.
    # Closes the "build_failing_qids_empty" decline path for the
    # typical-production case (Plan 11 ON, Plan 3 typed evidence empty,
    # but eval_results carries the failing rows). Explicit pre-stamping
    # by a caller wins — we only populate when truly absent so future
    # per-iteration stamping takes precedence.
    if metadata_snapshot.get("_failing_qids") is None:
        _stamp_failing_qids_from_eval_results(eval_results, metadata_snapshot)

    plan12_failing_qids = list(metadata_snapshot.get("_failing_qids") or [])
    if not plan12_failing_qids and rca_evidence_typed:
        # Last-ditch: derive from rca_evidence_typed if both the
        # iteration scope and the eval-results derivation came back
        # empty (e.g. eval_results was malformed).
        plan12_failing_qids = list(rca_evidence_typed.keys())

    plan11_clusters = _decide_and_run_plan11_dispatch(
        failing_qids=plan12_failing_qids,
        rca_evidence_typed=rca_evidence_typed or {},
        metadata_snapshot=metadata_snapshot,
        namespace=namespace or ("hard" if signal_type == "hard" else "soft"),
        signal_type=signal_type,
        run_id=run_id or "",
        w=w,
    )
    if plan11_clusters:
        return plan11_clusters
    # Plan 11 declined / errored / skipped — fall through to Plan 4 (or
    # the heuristic). The dispatch marker carries the diagnostic; the
    # Stage 1/2 markers carry the abstain reason when LLM ran.

    # ── Plan 4 — LLM-driven short-circuit. ─────────────────────────────
    # Lazy-imported to avoid load-order coupling with cluster_llm (which
    # imports rca_evidence_typed, which is part of the optimization
    # package this file lives in). When the flag is off OR no typed
    # evidence is supplied, fall through to the heuristic body verbatim.
    if (
        plan4_llm_clustering_enabled()
        and rca_evidence_typed
        and len(rca_evidence_typed) >= 2
    ):
        from genie_space_optimizer.optimization.cluster_llm import (
            cluster_failures_llm,
        )
        schema_columns: set[str] = set(
            metadata_snapshot.get("schema_columns") or []
        )
        if not schema_columns:
            for ev in rca_evidence_typed.values():
                schema_columns.update(ev.blame_set)
        _namespace = namespace or ("H" if signal_type == "hard" else "S")
        _iteration = int(metadata_snapshot.get("iteration") or 0)

        llm_clusters = cluster_failures_llm(
            w=w,
            rca_evidence_typed=rca_evidence_typed,
            schema_columns=schema_columns,
            iteration=_iteration,
            namespace=_namespace,
        )
        if llm_clusters is not None:
            # Plan 9 Task 4 — stamp the typed LlmCluster objects on
            # metadata_snapshot so the harness can thread them through
            # generate_proposals_from_strategy (which forwards them
            # into _select_lever_5_holistic_path and the L6 short-circuit
            # at optimizer.py:14122). Merge into any existing map so
            # both the hard + soft passes contribute.
            try:
                existing = (
                    metadata_snapshot.get("_llm_clusters_by_cluster_id") or {}
                )
                merged = dict(existing)
                for c in llm_clusters:
                    merged[str(c.cluster_id)] = c
                metadata_snapshot["_llm_clusters_by_cluster_id"] = merged
            except Exception:
                logger.debug(
                    "Plan 9 Task 4 — stamping _llm_clusters_by_cluster_id "
                    "failed (non-fatal)",
                    exc_info=True,
                )
            return [
                c.to_legacy_dict(signal_type=signal_type) for c in llm_clusters
            ]
        # LLM declined / errored / produced no valid clusters. Per the
        # roadmap, prefer prior_clusters (previous iteration's output);
        # if not supplied, fall through to the heuristic body below.
        if prior_clusters is not None:
            return prior_clusters
        # Fall through to heuristic ↓ (safety-net invariant).

    uc_asi_map: dict[tuple[str, str], dict] = {}
    if spark and run_id and catalog and schema:
        try:
            uc_asi = read_asi_from_uc(spark, run_id, catalog, schema)
            for a in uc_asi:
                key = (a.get("question_id", ""), a.get("judge", ""))
                uc_asi_map[key] = a
            if uc_asi_map:
                logger.info("Enriched clustering with %d UC ASI records", len(uc_asi_map))
        except Exception:
            logger.debug("UC ASI enrichment failed", exc_info=True)

    failures: list[dict] = []
    table = None

    results_obj = eval_results.get("eval_result")
    if results_obj is not None and hasattr(results_obj, "tables"):
        table = results_obj.tables.get("eval_results")
    elif results_obj is not None and hasattr(results_obj, "eval_results"):
        table = results_obj.eval_results

    if table is None:
        rows = (
            eval_results.get("eval_results")
            or eval_results.get("rows")
            or eval_results.get("table")
        )
        if isinstance(rows, list):
            table = rows

    if table is None:
        return []

    try:
        import pandas as pd

        if hasattr(table, "iterrows"):
            rows_iter = [row.to_dict() for _, row in table.iterrows()]
        else:
            rows_iter = table if isinstance(table, list) else []
    except ImportError:
        rows_iter = table if isinstance(table, list) else []

    _held_out: set[str] = set(held_out_qids or set())

    # S2 (duplicate-qid dedup) — a benchmark table can legally contain two
    # rows with the same ``id`` (the C1 dedupe script is a one-shot cleanup,
    # not a runtime constraint). Before S2, ``question_profiles[qid]``
    # silently merged their failures, so the blame/counterfactual/root-cause
    # aggregation double-counted and the weighted dominant cause was biased
    # toward whichever row ran second. We now auto-suffix reappearing qids
    # with ``:v2`` / ``:v3`` when the request signature differs, and drop
    # pure duplicates (same qid, same question/expected_sql) with a log
    # entry. The rewrite log is surfaced in the CLUSTER FORMATION block.
    #
    # Tier 2.11: when the caller supplies ``qid_state``, the dedup caches
    # persist across calls. Hard+soft clustering share one state dict so
    # the same physical row gets one stable qid (possibly with :vN suffix)
    # and therefore one cluster, not two.
    if qid_state is not None:
        _qid_seen = qid_state.setdefault("seen", {})
        _qid_version = qid_state.setdefault("version", {})
        _qid_rewrites = qid_state.setdefault("rewrites", [])
        _qid_pure_duplicates = qid_state.setdefault("pure_duplicates", [])
    else:
        _qid_seen = {}
        _qid_version = {}
        _qid_rewrites = []
        _qid_pure_duplicates = []

    for row in rows_iter:
        if not isinstance(row, dict):
            continue

        # Bug #4 (P4.2) — held-out benchmarks must never enter the LLM-
        # bound clustering path. Drop them before any signal extraction.
        qid = str(row.get("question_id") or row.get("qid") or "")
        if qid and qid in _held_out:
            continue

        _req = row.get("request") or {}
        if isinstance(_req, str):
            try:
                _req = json.loads(_req)
            except (json.JSONDecodeError, TypeError):
                _req = {}
        _req_kwargs = _req.get("kwargs", {}) if isinstance(_req, dict) else {}
        _resp = row.get("response") or {}
        if isinstance(_resp, str):
            try:
                _resp = json.loads(_resp)
            except (json.JSONDecodeError, TypeError):
                _resp = {}

        question_id = _row_qid(row)

        # S2: disambiguate duplicate qids by request signature. Signature is
        # (question_text, expected_sql) — stable across the benchmark row
        # identity but unique across distinct rows that share an id.
        _row_question = _req.get("question", "") if isinstance(_req, dict) else ""
        _row_expected = _req.get("expected_sql", "") if isinstance(_req, dict) else ""
        _row_signature = (str(_row_question).strip(), str(_row_expected).strip())
        # Tier 2.12: capture the base question_id (the real benchmark id)
        # separately so ``:vN`` tokens never propagate into outward-facing
        # fields. Downstream consumers that need to map back to a benchmark
        # row (slice sampler, benchmark lookup, UI drill-down, human-review
        # flagging) must use base_question_id — not question_id.
        _base_qid = question_id
        _trial_index = 1
        if question_id in _qid_seen:
            if _qid_seen[question_id] == _row_signature:
                _qid_pure_duplicates.append(question_id)
                continue
            version = _qid_version.get(question_id, 1) + 1
            _qid_version[question_id] = version
            _rewritten = f"{question_id}:v{version}"
            _qid_rewrites.append(f"{question_id} -> {_rewritten}")
            _trial_index = version
            question_id = _rewritten
            _qid_seen[question_id] = _row_signature
        else:
            _qid_seen[question_id] = _row_signature
            _qid_version[question_id] = 1

        sql_ctx = {
            "question": _req.get("question", "") if isinstance(_req, dict) else "",
            "expected_sql": _req.get("expected_sql", "") if isinstance(_req, dict) else "",
            "generated_sql": _resp.get("response", "") if isinstance(_resp, dict) else "",
            "comparison": _resp.get("comparison", {}) if isinstance(_resp, dict) else {},
        }

        _NON_JUDGE_SUFFIXES = ("/rationale", "/source", "/metadata", "/error")
        for col_name, val in list(row.items()):
            judge: str | None = None
            if col_name.startswith("feedback/") and col_name.endswith("/value"):
                judge = col_name.removeprefix("feedback/").removesuffix("/value")
            elif col_name.startswith("feedback/") and not any(col_name.endswith(s) for s in _NON_JUDGE_SUFFIXES):
                bare = col_name.removeprefix("feedback/")
                if "/" not in bare:
                    judge = bare
            elif col_name.endswith("/value") and not col_name.startswith("feedback/"):
                judge = col_name.removesuffix("/value")
            if judge and "no" in str(val).lower():
                from genie_space_optimizer.optimization.evaluation import (
                    _parse_asi_from_rationale,
                )
                rationale = (
                    row.get(f"feedback/{judge}/rationale")
                    or row.get(f"{judge}/rationale")
                    or row.get(f"rationale/{judge}")
                    or row.get("rationale", "")
                )
                # T0.1: instrument ASI extraction paths so operators can
                # see *why* ASI metadata is or is not found per judge.
                # In iteration-3 of the retail corpus, 100% of failures
                # logged ``ASI metadata found: NO`` even though the
                # scorers emit metadata. Log which source produced
                # ``judge_meta`` so we can tell "scorer didn't emit it"
                # apart from "MLflow didn't propagate it" apart from
                # "rationale parse fallback worked".
                _asi_source = "none"
                # Phase 2.3: respect the row-level ``_asi_source``
                # breadcrumb stamped by ``_merge_row_sources`` so the
                # histogram correctly reports ``trace`` /
                # ``recovered_trace`` / ``cache`` instead of always
                # collapsing to ``row_metadata``.
                _row_level_source = row.get("_asi_source") or ""
                judge_meta = row.get(f"feedback/{judge}/metadata")
                if judge_meta:
                    _asi_source = "feedback_metadata"
                else:
                    judge_meta = row.get(f"{judge}/metadata")
                    if judge_meta:
                        _asi_source = (
                            f"row_metadata:{_row_level_source}"
                            if _row_level_source else "row_metadata"
                        )
                    else:
                        judge_meta = {}
                if not isinstance(judge_meta, dict):
                    try:
                        judge_meta = json.loads(judge_meta) if isinstance(judge_meta, str) else {}
                        if judge_meta:
                            _asi_source = f"{_asi_source}_json_parsed"
                    except (json.JSONDecodeError, TypeError):
                        judge_meta = {}
                        _asi_source = f"{_asi_source}_json_parse_failed"
                if not judge_meta:
                    _parsed = _parse_asi_from_rationale(rationale)
                    if _parsed:
                        judge_meta = _parsed
                        _asi_source = "rationale_parse"
                # Stash source on the row so downstream cluster-
                # formation debug logs can surface "n/m failures had
                # asi via feedback_metadata" vs "... via rationale_parse".
                row.setdefault("_asi_extraction_log", []).append({
                    "judge": judge,
                    "source": _asi_source,
                    "found": bool(judge_meta),
                })
                asi_failure_type = (
                    judge_meta.get("failure_type")
                    or row.get(f"metadata/{judge}/failure_type")
                    or row.get("metadata/failure_type")
                )
                asi_blame_set = (
                    judge_meta.get("blame_set")
                    or row.get(f"metadata/{judge}/blame_set")
                    or row.get("metadata/blame_set")
                )
                asi_counterfactual = (
                    judge_meta.get("counterfactual_fix")
                    or row.get(f"metadata/{judge}/counterfactual_fix")
                    or row.get("metadata/counterfactual_fix")
                )
                asi_wrong_clause = (
                    judge_meta.get("wrong_clause")
                    or row.get(f"metadata/{judge}/wrong_clause")
                )

                asi_join_assessment = judge_meta.get("join_assessment")

                if not asi_failure_type and uc_asi_map:
                    uc_asi_entry = uc_asi_map.get((question_id, judge))
                    if uc_asi_entry:
                        asi_failure_type = asi_failure_type or uc_asi_entry.get("failure_type")
                        asi_blame_set = asi_blame_set or uc_asi_entry.get("blame_set")
                        asi_counterfactual = asi_counterfactual or uc_asi_entry.get("counterfactual_fix")
                        asi_wrong_clause = asi_wrong_clause or uc_asi_entry.get("wrong_clause")
                        if not asi_join_assessment:
                            asi_join_assessment = uc_asi_entry.get("join_assessment")

                # Tier 2.13 / 2.14 (narrowed by Task 7): fall back to the
                # Genie-behaviour-pattern classifier ONLY when ASI carries
                # no specific failure_type. ``wrong_filter_condition`` and
                # ``wrong_aggregation`` are themselves specific labels —
                # the typed ``SqlDiff`` produced by Task 6 supplies the
                # finer routing signal, so the override is reserved for
                # ``other`` / missing labels. This stops the override
                # from flattening five generic ``other`` verdicts onto
                # the same pattern label and inflating dominance, the
                # G1 failure mode the retail run exhibited.
                if not asi_failure_type or asi_failure_type == "other":
                    try:
                        from genie_space_optimizer.optimization.evaluation import (
                            classify_genie_shape_patterns,
                        )
                        _pattern = classify_genie_shape_patterns(row)
                        if _pattern:
                            asi_failure_type = _pattern["failure_type"]
                            if not asi_blame_set:
                                asi_blame_set = _pattern.get("blame_set")
                            if not asi_wrong_clause:
                                asi_wrong_clause = _pattern.get("wrong_clause")
                    except Exception:
                        logger.debug("classify_genie_shape_patterns raised", exc_info=True)

                # T1.3: signal-quality metadata. Each failure carries
                # booleans describing *how* its attribution was derived
                # so cluster impact and the strategist can discount
                # low-quality signals. Ratio of trusted signals is used
                # by ``cluster_impact`` to scale the dampen multiplier.
                _asi_present = bool(
                    asi_failure_type and asi_failure_type not in ("other", "")
                )
                _result_fetched = True
                _cmp = (sql_ctx or {}).get("comparison") or {}
                _err_type = str(_cmp.get("error_type") or "").lower()
                if _err_type in ("genie_result_unavailable",):
                    _result_fetched = False

                failure_entry: dict = {
                    "question_id": question_id,
                    "base_question_id": _base_qid,
                    "trial_index": _trial_index,
                    "judge": judge,
                    "rationale": rationale,
                    "asi_failure_type": asi_failure_type,
                    "asi_blame_set": asi_blame_set,
                    "asi_counterfactual_fix": asi_counterfactual,
                    "asi_wrong_clause": asi_wrong_clause,
                    "sql_context": sql_ctx,
                    # T1.3: signal-quality metadata.
                    "_signal_quality": {
                        "asi_present": _asi_present,
                        "result_fetched": _result_fetched,
                    },
                }
                if isinstance(asi_join_assessment, dict) and asi_join_assessment.get("left_table"):
                    failure_entry["asi_join_assessment"] = asi_join_assessment
                _ast_pairs = _normalize_ast_diff_sql_pairs(
                    row.get("_sql_pairs_for_ast_diff")
                )
                if _ast_pairs:
                    failure_entry["_sql_pairs_for_ast_diff"] = _ast_pairs
                if row.get("_feature_diff") is not None:
                    failure_entry["_feature_diff"] = row["_feature_diff"]
                failures.append(failure_entry)

    question_profiles: dict[str, dict] = {}
    for f in failures:
        qid = f["question_id"]
        if qid not in question_profiles:
            question_profiles[qid] = {
                "judges": set(),
                "root_causes": [],
                "blame_sets": [],
                "counterfactual_fixes": [],
                "wrong_clauses": [],
                "sql_context": f.get("sql_context", {}),
                "sql_pairs_for_ast_diff": [],
                "feature_diffs": [],
                "failures": [],
                # Tier 2.12: preserve the benchmark's real id so downstream
                # outward-facing fields (ag.affected_questions, provenance,
                # slice samplers, UI) never have to strip ``:vN`` tokens.
                "base_question_id": f.get("base_question_id", qid),
                "trial_index": f.get("trial_index", 1),
            }
        profile = question_profiles[qid]
        profile["judges"].add(f["judge"])
        profile["failures"].append(f)
        for _pair in f.get("_sql_pairs_for_ast_diff") or []:
            if _pair not in profile["sql_pairs_for_ast_diff"]:
                profile["sql_pairs_for_ast_diff"].append(_pair)
        if f.get("_feature_diff") is not None:
            profile["feature_diffs"].append(f["_feature_diff"])

        # S3 — Root-cause cascade, ordered so each level overrides later
        # ones. The historical bug was that empty ``generated_sql`` fell
        # through to ``_classify_sql_diff`` which then emitted nonsense
        # ``missing_filter`` / ``wrong_join`` labels based on the absence
        # of a WHERE clause, and that ASI ``failure_type="other"`` with
        # a non-empty ``blame_set`` was discarded instead of rescued into
        # ``missing_data_asset`` (Lever 3).
        sql_context = f.get("sql_context", {}) or {}
        generated_sql = str(sql_context.get("generated_sql", "") or "").strip()
        asi_ft = f.get("asi_failure_type")

        # P1.3 — pass metadata snapshot through so the
        # ``column_disambiguation`` matcher can corroborate same-type
        # candidates without re-reading the snapshot from disk.
        if metadata_snapshot and "metadata_snapshot" not in sql_context:
            sql_context["metadata_snapshot"] = metadata_snapshot

        if not generated_sql:
            root = "missing_sql_generation"
            resolution_method = "empty_sql_shortcut"
        elif asi_ft and asi_ft != "other":
            root = asi_ft
            resolution_method = "asi_metadata"
        else:
            # P1.3 — promote structural pattern detection above the
            # blame_set / rationale branches so a high-confidence
            # pattern (>= 0.7) wins over the generic
            # ``missing_data_asset`` / rationale-derived bucket. The
            # ASI-metadata branch above still wins when present;
            # patterns only run when ASI is absent or generic.
            _pattern_label, _pattern_conf, _pattern_evidence = (
                _detect_failure_pattern(sql_context)
            )
            if _pattern_label and _pattern_conf >= 0.7:
                root = _pattern_label
                resolution_method = "structural_pattern"
                sql_context.setdefault("_pattern_label", _pattern_label)
                sql_context.setdefault("_pattern_confidence", _pattern_conf)
                sql_context.setdefault("_pattern_evidence", _pattern_evidence)
            elif (
                (asi_ft == "other" or not asi_ft)
                and _blame_set_matches_metadata(f.get("asi_blame_set"), metadata_snapshot)
            ):
                root = "missing_data_asset"
                resolution_method = "asi_blame_set"
            else:
                pattern = _extract_pattern(f["rationale"])
                if pattern != "other":
                    root = pattern
                    resolution_method = "rationale_pattern"
                else:
                    root = _classify_sql_diff(sql_context)
                    resolution_method = "sql_diff"
        f["_resolved_root_cause"] = root
        f["_resolution_method"] = resolution_method

        # T1.1: build a structured FailureAttribution record alongside
        # the legacy ``_resolved_root_cause`` string so downstream can
        # reason about attribution source, confidence, and competing
        # hypotheses without re-deriving them. This is the foundation
        # for pattern-aware clustering (T1.2) and signal-quality
        # weighting (T1.3) — both now have a single canonical place
        # to read from and stamp into.
        _attrib_confidence: float
        if resolution_method == "asi_metadata":
            _attribution_source = "trace_evidence"
            _attrib_confidence = 0.9
        elif resolution_method == "asi_blame_set":
            _attribution_source = "trace_evidence"
            _attrib_confidence = 0.8
        elif resolution_method == "structural_pattern":
            # P1.3 — promoted above blame_set / rationale; the pattern
            # detector returned its own confidence which is already
            # stamped on sql_context. Read it back here so the
            # attribution record carries the matcher's confidence
            # rather than a flat default.
            _attribution_source = "sql_diff"
            _attrib_confidence = float(
                sql_context.get("_pattern_confidence", 0.75)
            )
        elif resolution_method == "rationale_pattern":
            _attribution_source = "judge_text"
            _attrib_confidence = 0.65
        elif resolution_method == "sql_diff":
            _attribution_source = "sql_diff"
            # When the T1.2 pattern detector fires, the ctx was stamped
            # with ``_pattern_confidence`` — use it so richer patterns
            # read as higher-confidence attribution than the legacy
            # bucket labels.
            _attrib_confidence = float(
                sql_context.get("_pattern_confidence", 0.5)
            )
        elif resolution_method == "empty_sql_shortcut":
            _attribution_source = "heuristic"
            _attrib_confidence = 1.0
        else:
            _attribution_source = "heuristic"
            _attrib_confidence = 0.4

        # Pull target asset from ASI blame_set (preferred — it names the
        # exact metadata object) or pattern evidence (fallback — names
        # the general bucket). Target kind is inferred from the asset
        # shape: ``table.column`` → column; ``db.schema.table`` →
        # table; etc.
        _target_asset_id: str | None = None
        _target_asset_kind: str | None = None
        _blame_for_target = f.get("asi_blame_set")
        if _blame_for_target:
            if isinstance(_blame_for_target, list) and _blame_for_target:
                _target_asset_id = str(_blame_for_target[0]).strip()
            elif isinstance(_blame_for_target, str):
                _target_asset_id = _blame_for_target.strip()
            if _target_asset_id:
                _n_dots = _target_asset_id.count(".")
                if _n_dots >= 3:
                    _target_asset_kind = "column"
                elif _n_dots == 2:
                    _target_asset_kind = "table"
                else:
                    _target_asset_kind = "pattern"

        f["_attribution"] = {
            "target_asset_kind": _target_asset_kind,
            "target_asset_id": _target_asset_id,
            "pattern_label": sql_context.get("_pattern_label") or None,
            "attribution_source": _attribution_source,
            "confidence": round(_attrib_confidence, 3),
            "evidence_snippet": str(
                sql_context.get("_pattern_evidence") or f.get("rationale", "")
            )[:300],
        }

        profile["root_causes"].append(root)

        if f.get("asi_blame_set"):
            from genie_space_optimizer.optimization.blame_normalization import (
                normalize_blame_set,
            )

            for token in normalize_blame_set(f["asi_blame_set"]):
                if token and token not in profile["blame_sets"]:
                    profile["blame_sets"].append(token)
        if f.get("asi_counterfactual_fix"):
            profile["counterfactual_fixes"].append(f["asi_counterfactual_fix"])
        if f.get("asi_wrong_clause"):
            profile["wrong_clauses"].append(f["asi_wrong_clause"])

    # ── Filter-aware blame adjustment: clear blame for instruction-mandated filters
    default_filters = _extract_instruction_default_filters(metadata_snapshot)
    _default_filter_cols = {f["column"] for f in default_filters}
    if _default_filter_cols:
        for qid, profile in question_profiles.items():
            _adjusted = []
            for blame_item in profile["blame_sets"]:
                blame_lower = blame_item.lower()
                if any(col in blame_lower for col in _default_filter_cols):
                    logger.debug(
                        "Clearing blame '%s' for Q=%s — matches instruction default filter",
                        blame_item, qid,
                    )
                    continue
                _adjusted.append(blame_item)
            if len(_adjusted) != len(profile["blame_sets"]):
                profile["blame_sets"] = _adjusted

    # Phase B2: weighted dominant-root-cause selection.
    # Each judge gets a weight reflecting its signal class (SQL_SHAPE = 1.0,
    # ROUTING = 0.5, NL_TEXT = 0.1, META/INFRA = 0.0). We sum those weights
    # per root cause and take the heaviest, breaking ties in favor of
    # SQL-shape causes so a single NL-text vote can't override multiple
    # SQL-judge votes (the Q004 regression pattern).
    from genie_space_optimizer.optimization.judge_classes import (
        judge_weight_for_root_cause,
    )

    for qid, profile in question_profiles.items():
        weighted: dict[str, float] = defaultdict(float)
        for f in profile["failures"]:
            cause = f.get("_resolved_root_cause", "other")
            weighted[cause] += judge_weight_for_root_cause(f.get("judge", ""))
        if weighted:
            profile["dominant_root_cause"] = _select_dominant_root_cause(weighted)
            profile["dominant_root_cause_weight"] = round(
                weighted[profile["dominant_root_cause"]], 3,
            )
        else:
            profile["dominant_root_cause"] = "other"
            profile["dominant_root_cause_weight"] = 0.0

    # ── 8a. Per-Question ASI Extraction Trace ───────────────────────────
    _cluster_debug = os.environ.get("CLUSTER_DEBUG", "1").lower() not in ("0", "false", "no")
    # T3.15: disambiguate duplicate headers — the hard and soft passes
    # both call cluster_failures with identical banner text, making log
    # navigation ambiguous. Tag each banner with the signal_type.
    _pass_tag = "HARD FAILURES" if signal_type == "hard" else "SOFT SIGNALS"
    if _cluster_debug and question_profiles:
        # T0.1: aggregate source histogram so operators immediately see
        # why ASI metadata is or isn't being found.
        _asi_source_counts: dict[str, int] = {}
        _total_judge_failures = 0
        for _p in question_profiles.values():
            for _f in _p.get("failures", []) or []:
                _sctx = _f.get("sql_context") or {}
                # _asi_extraction_log was stamped on the ROW not the
                # failure; best-effort lookup via the failure payload.
                # When absent (older paths), we still categorise as
                # "unknown" so the histogram denominator is consistent
                # with the judge-failure count.
                _total_judge_failures += 1
        # The log lives on the source row. Walk rows_df again if we
        # still have them — otherwise aggregate is skipped (pattern is
        # still visible per-row in verbose mode).
        lines = [f"\n== ASI EXTRACTION TRACE ({_pass_tag}) =========================================="]
        # Emit the aggregate source histogram — collected from
        # ``_asi_extraction_log`` stamps on each source row (if the
        # row dicts survived to here). When callers provide ``rows``
        # in the eval_results we re-walk them.
        _src_hist: dict[str, int] = {}
        for _row in (eval_results.get("rows") or []):
            for _log in _row.get("_asi_extraction_log") or []:
                _src = str(_log.get("source", "unknown"))
                _src_hist[_src] = _src_hist.get(_src, 0) + 1
        if _src_hist:
            _total = sum(_src_hist.values())
            _hist_parts = [
                f"{k}={v} ({100*v/_total:.0f}%)"
                for k, v in sorted(_src_hist.items(), key=lambda kv: -kv[1])
            ]
            lines.append(
                "|  ASI source histogram:     "
                + ", ".join(_hist_parts)
                + f"  (total judge failures: {_total})"
            )
        if verbose:
            for qid, profile in question_profiles.items():
                lines.append(f"\n--- Q: {qid} " + "-" * max(1, 60 - len(qid)))
                for f in profile["failures"]:
                    judge = f["judge"]
                    verdict = "FAIL"
                    asi_ft = f.get("asi_failure_type")
                    blame = f.get("asi_blame_set")
                    cfix = f.get("asi_counterfactual_fix")
                    wclause = f.get("asi_wrong_clause")
                    resolved = f.get("_resolved_root_cause", "other")
                    method = f.get("_resolution_method", "unknown")
                    lines.append(f"|  Judge: {judge:<24s}|  Verdict: {verdict}")
                    has_asi = bool(asi_ft)
                    lines.append(f"|    ASI metadata found:      {'YES' if has_asi else 'NO'}")
                    if has_asi:
                        lines.append(f"|      failure_type (raw):    {asi_ft}")
                        if blame:
                            lines.append(f"|      blame_set:             {blame}")
                        if cfix:
                            lines.append(f"|      counterfactual_fix:    \"{str(cfix)[:120]}\"")
                        if wclause:
                            lines.append(f"|      wrong_clause:          {wclause}")
                    lines.append(f"|    Final root cause:        {resolved}  (via {method})")
                _dom = profile['dominant_root_cause']
                _dom_w = profile.get('dominant_root_cause_weight', 0.0)
                lines.append(f"|  Dominant root cause:       {_dom} (weight={_dom_w})")
                blame_key = "|".join(sorted(profile["blame_sets"])) if profile["blame_sets"] else "(none)"
                lines.append(f"|  Cluster group key:         ({_dom}, \"{blame_key}\")")
        else:
            lines.append(f"|  (compact mode — {len(question_profiles)} questions)")
            for qid, profile in question_profiles.items():
                judges = ", ".join(sorted(profile["judges"]))
                blame_key = "|".join(sorted(profile["blame_sets"])) if profile["blame_sets"] else "(none)"
                lines.append(
                    f"|  {qid}: root={profile['dominant_root_cause']}  "
                    f"judges=[{judges}]  blame={blame_key}"
                )
        lines.append("-" * 78)
        print("\n".join(lines))

    cluster_groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for qid, profile in question_profiles.items():
        blame_key = "|".join(sorted(profile["blame_sets"])) if profile["blame_sets"] else ""
        root = profile["dominant_root_cause"]
        if root in ("other", "unverifiable_no_expected_sql") and not blame_key:
            ctx = profile.get("sql_context", {})
            gen_sql = (ctx.get("response", {}) or {}).get("response", "") if isinstance(ctx.get("response"), dict) else ""
            if not gen_sql:
                gen_sql = str(ctx.get("generated_sql", ""))
            q_text = ctx.get("question", "") if ctx else ""
            sub_root = _classify_generated_sql_quality(gen_sql, q_text) if gen_sql else root
            if sub_root != "unverifiable_no_expected_sql":
                root = sub_root
        group_key = (root, blame_key)
        cluster_groups[group_key].append(qid)

    # Phase B3: when the cluster's dominant root cause is SQL-shape, we
    # suppress counterfactuals whose source judge is NL_TEXT or META from
    # the strategist-facing summary. Per-judge rows are unchanged — they
    # still land in ``question_traces`` and in the ASI Delta table so
    # forensics tools see the full picture. The Q004 motivating case:
    # response_quality's "don't fabricate numbers" counterfactual was
    # outvoting four SQL-shape judges pointing at missing_filter.
    from genie_space_optimizer.optimization.judge_classes import (
        SignalClass,
        judge_signal_class,
    )

    clusters: list[dict] = []
    for (root_cause, blame_str), qids in cluster_groups.items():
        all_judges: set[str] = set()
        _judge_fail_counts: dict[str, int] = {}
        # Tier 2.15: all_counterfactuals is now a list of dicts tagged
        # with judge + signal_class so we can apply SQL-shape suppression
        # _after_ merging across signal types. The pre-aggregation
        # suppression historically caused clusters to show "(none)" even
        # when a counterfactual existed on a sibling pathway.
        all_counterfactuals: list[dict] = []
        all_wrong_clauses: list[str] = []
        sql_contexts: list[dict] = []
        sample_asi_type: str | None = None
        join_assessments: list[dict] = []
        sql_pairs_for_ast_diff: list[dict[str, str]] = []
        feature_diffs: list[Any] = []

        _cluster_is_sql_shape = root_cause in _SQL_SHAPE_ROOT_CAUSES
        _suppress_classes = {SignalClass.NL_TEXT, SignalClass.META}

        question_traces: list[dict] = []
        for qid in qids:
            profile = question_profiles[qid]
            all_judges.update(profile["judges"])
            # Tier 2.2: dominance counting. Track each judge's failure frequency
            # across this cluster's profiles so ``affected_judge`` reflects the
            # judge that actually drove the cluster, not the alphabetically
            # first one.
            for _f in profile["failures"]:
                _jn = _f.get("judge", "")
                if _jn:
                    _judge_fail_counts[_jn] = _judge_fail_counts.get(_jn, 0) + 1
            all_wrong_clauses.extend(profile["wrong_clauses"])
            # Tier 2.15: merge counterfactual fixes across all signal
            # classes first, then apply the SQL-shape suppression ONLY if
            # the merge still has preferred (non-NL-text) entries. This
            # prevents the "(none)" display bug where a cluster shows no
            # counterfactual even though a sibling signal type on the same
            # base_question_id had one populated. The suppression block
            # below runs once we've seen all failures, not per-failure.
            for f in profile["failures"]:
                cf = f.get("asi_counterfactual_fix")
                if not cf:
                    continue
                _signal_class = judge_signal_class(f.get("judge", ""))
                all_counterfactuals.append({
                    "fix": cf,
                    "judge": f.get("judge", ""),
                    "signal_class": _signal_class,
                    "base_question_id": f.get("base_question_id") or profile.get("base_question_id"),
                })
            if profile["sql_context"]:
                sql_contexts.append(profile["sql_context"])
            for _pair in profile.get("sql_pairs_for_ast_diff", []) or []:
                if _pair not in sql_pairs_for_ast_diff:
                    sql_pairs_for_ast_diff.append(_pair)
            feature_diffs.extend(profile.get("feature_diffs", []) or [])
            if not sample_asi_type:
                for f in profile["failures"]:
                    if f.get("asi_failure_type"):
                        sample_asi_type = f["asi_failure_type"]
                        break
            for f in profile["failures"]:
                ja = f.get("asi_join_assessment")
                if isinstance(ja, dict) and ja.get("left_table"):
                    join_assessments.append({**ja, "question_id": qid})
            q_text = profile["sql_context"].get("question", "") if profile["sql_context"] else ""
            judge_traces = []
            for f in profile["failures"]:
                judge_traces.append({
                    "judge": f["judge"],
                    "verdict": "FAIL",
                    "asi_failure_type_raw": f.get("asi_failure_type"),
                    "resolved_root_cause": f.get("_resolved_root_cause", "other"),
                    "resolution_method": f.get("_resolution_method", "unknown"),
                    "blame_set": f.get("asi_blame_set"),
                    "counterfactual_fix": f.get("asi_counterfactual_fix"),
                    "wrong_clause": f.get("asi_wrong_clause"),
                    "rationale_snippet": (f.get("rationale") or "")[:500],
                })
            question_traces.append({
                "question_id": qid,
                "base_question_id": profile.get("base_question_id", qid),
                "trial_index": profile.get("trial_index", 1),
                "question_text": q_text[:200],
                "failed_judges": judge_traces,
            })

        unique_qids = sorted(set(qids))
        # Tier 2.12: compute base_question_ids — the real benchmark ids,
        # with :vN tokens stripped. Downstream consumers that map back to
        # benchmark rows (slice sampler, UI drill-down, human-review
        # flagging) use this list.
        _base_qids: set[str] = set()
        for _pid in unique_qids:
            _prof = question_profiles.get(_pid)
            if _prof:
                _base_qids.add(str(_prof.get("base_question_id") or _pid))
            else:
                _base_qids.add(_pid.split(":v")[0])

        # Tier 2.2: pick affected_judge by dominance (most failures),
        # breaking ties by CAUSAL_WEIGHT and then alphabetical (stable).
        # The previous alphabetical pick produced mislabelled clusters
        # (e.g. soft cluster with 12 response_quality failures labelled
        # ``completeness`` because ``c`` sorts before ``r``). That label
        # then feeds ``cluster_impact`` via ``CAUSAL_WEIGHT``, so the
        # mislabel propagates into prioritisation errors.
        if all_judges:
            from genie_space_optimizer.common.config import CAUSAL_WEIGHT
            _dominant = max(
                all_judges,
                key=lambda j: (
                    _judge_fail_counts.get(j, 0),
                    CAUSAL_WEIGHT.get(j, 1.0),
                    -ord(j[0]) if j else 0,
                ),
            )
        else:
            _dominant = "unknown"

        # Tier 2.15: merge counterfactuals across all signal classes, then
        # apply the SQL-shape suppression only if there's still a useful
        # non-NL-text entry left. If every counterfactual came from an
        # NL_TEXT / META judge, keep them — better to show the
        # strategist something (even if weak) than "(none)".
        _preferred_cfs = [
            c for c in all_counterfactuals
            if not _cluster_is_sql_shape or c.get("signal_class") not in _suppress_classes
        ]
        _merged_cf_source = _preferred_cfs if _preferred_cfs else all_counterfactuals
        _merged_cfs = list(dict.fromkeys(
            str(c.get("fix", "")).strip()
            for c in _merged_cf_source
            if c.get("fix") and str(c.get("fix")).strip()
        ))
        _cf_sources = sorted({
            c.get("signal_class").name if hasattr(c.get("signal_class"), "name") else str(c.get("signal_class"))
            for c in _merged_cf_source
            if c.get("signal_class")
        })

        _ns = namespace if namespace else ("H" if signal_type == "hard" else "S")
        # T1.12: precompute judge_failure_ratio per question, where the
        # denominator is the count of NON-info judges (CAUSAL_WEIGHT keys
        # minus INFO_ONLY_JUDGES) and the numerator is the number of
        # non-info judges that failed for that question. The cluster-level
        # mean drives the ``rank_clusters`` soft re-elevation decision.
        from genie_space_optimizer.common.config import (
            CAUSAL_WEIGHT as _CAUSAL_W,
            INFO_ONLY_JUDGES as _INFO_ONLY,
        )
        _non_info_judges = {j for j in _CAUSAL_W.keys() if j not in _INFO_ONLY}
        _non_info_total = max(len(_non_info_judges), 1)
        _ratios: list[float] = []
        for _qid in unique_qids:
            _pj = question_profiles.get(_qid, {}).get("judges", set()) or set()
            _failing_non_info = {j for j in _pj if j in _non_info_judges}
            _ratios.append(len(_failing_non_info) / _non_info_total)
        _mean_jfr = sum(_ratios) / len(_ratios) if _ratios else 0.0

        # T1.3: compute cluster-level signal quality from the per-failure
        # ``_signal_quality`` fields stamped in the failures list.
        # ``asi_ratio`` is the fraction of failures backed by real ASI
        # metadata (vs heuristic sql_diff); ``result_fetched_ratio``
        # tracks whether Genie actually returned results. A cluster with
        # low signal quality is less trusted by the strategist and
        # cluster_impact.
        _asi_flags: list[bool] = []
        _result_flags: list[bool] = []
        _attrib_sources: list[str] = []
        _attrib_confidences: list[float] = []
        _pattern_labels: list[str] = []
        for _qid in unique_qids:
            _prof_failures = question_profiles.get(_qid, {}).get("failures", []) or []
            for _f in _prof_failures:
                _sq = _f.get("_signal_quality") or {}
                _asi_flags.append(bool(_sq.get("asi_present")))
                _result_flags.append(bool(_sq.get("result_fetched", True)))
                _attr = _f.get("_attribution") or {}
                _src = _attr.get("attribution_source")
                if _src:
                    _attrib_sources.append(str(_src))
                _attrib_confidences.append(float(_attr.get("confidence", 0.0)))
                _pl = _attr.get("pattern_label")
                if _pl:
                    _pattern_labels.append(str(_pl))
        _asi_ratio = (
            sum(_asi_flags) / len(_asi_flags) if _asi_flags else 0.0
        )
        _result_fetched_ratio = (
            sum(_result_flags) / len(_result_flags) if _result_flags else 1.0
        )
        # T1.1: aggregate attribution summary for the cluster — which
        # sources did the failures come from, what's the mean
        # confidence, which pattern labels fired? Exposed on
        # cluster["attribution"] so the strategist prompt can show
        # "this cluster is backed by 3 trace-evidence, 2 sql_diff"
        # instead of a generic root_cause label.
        _source_counts: dict[str, int] = {}
        for _s in _attrib_sources:
            _source_counts[_s] = _source_counts.get(_s, 0) + 1
        _dominant_pattern: str | None = None
        if _pattern_labels:
            _pat_counts: dict[str, int] = {}
            for _pl in _pattern_labels:
                _pat_counts[_pl] = _pat_counts.get(_pl, 0) + 1
            _dominant_pattern = max(
                _pat_counts.items(), key=lambda kv: kv[1]
            )[0]

        # T2.1: cluster_signature is an iteration-independent identity
        # hash that joins the cluster to its own history across runs.
        # It's keyed on the base_question_ids + root_cause + asi blame
        # so "the same cluster" (same failing questions, same blame)
        # keeps the same signature even as cluster_id churns by
        # iteration. Downstream readers (strategist, persistence
        # summary) can use this to load prior-attempt outcomes for
        # this same cluster without pattern-matching on the pretty ID.
        import hashlib as _hashlib
        _blame_sig = "|".join(
            sorted(b.strip() for b in blame_str.split("|") if b.strip())
        ) if blame_str else ""
        _sig_parts = (
            "|".join(sorted(_base_qids)),
            root_cause or "",
            _blame_sig,
        )
        _sig_bytes = "||".join(_sig_parts).encode("utf-8")
        _cluster_signature = _hashlib.sha1(_sig_bytes).hexdigest()[:16]

        entry = {
            "cluster_id": f"{_ns}{len(clusters) + 1:03d}",
            "cluster_signature": _cluster_signature,
            "root_cause": root_cause,
            "question_ids": unique_qids,
            "base_question_ids": sorted(_base_qids),
            "affected_judges": sorted(all_judges),
            "affected_judge": _dominant,
            "affected_judge_fail_counts": dict(_judge_fail_counts),
            "confidence": min(0.9, 0.5 + 0.1 * len(unique_qids)),
            "signal_type": signal_type,
            "asi_failure_type": sample_asi_type,
            "asi_blame_set": (
                list(
                    __import__(
                        "genie_space_optimizer.optimization.blame_normalization",
                        fromlist=["normalize_blame_set"],
                    ).normalize_blame_set(
                        [b.strip() for b in blame_str.split("|") if b.strip()]
                    )
                ) if blame_str else None
            ),
            "asi_wrong_clause": next((wc for wc in all_wrong_clauses if wc), None),
            "asi_counterfactual_fixes": _merged_cfs,
            "asi_counterfactual_sources": _cf_sources,
            "sql_contexts": sql_contexts[:5],
            "question_traces": question_traces,
            "mean_judge_failure_ratio": round(_mean_jfr, 4),
            # T1.3: cluster signal-quality summary. Low values mean the
            # cluster was assembled mostly from heuristic sql_diff
            # attribution rather than real trace ASI, and/or from
            # questions whose Genie results couldn't be fetched. Used
            # by ``cluster_impact`` to dampen low-confidence clusters
            # and surfaced in the strategist prompt so the LLM can
            # prefer evidence-backed clusters.
            "signal_quality": {
                "asi_ratio": round(_asi_ratio, 3),
                "result_fetched_ratio": round(_result_fetched_ratio, 3),
                "combined": round(
                    0.6 * _asi_ratio + 0.4 * _result_fetched_ratio, 3,
                ),
            },
            # T1.1: aggregate attribution provenance for the cluster.
            # ``source_counts`` tells the strategist what flavour of
            # evidence backs this cluster ("mostly trace_evidence" vs
            # "mostly heuristic"); ``mean_confidence`` summarises how
            # sure we are; ``dominant_pattern_label`` names the
            # specific pattern (e.g. ``time_window_pivot``) when one
            # has a clear plurality.
            "attribution": {
                "source_counts": dict(_source_counts),
                "mean_confidence": round(
                    sum(_attrib_confidences) / len(_attrib_confidences), 3,
                ) if _attrib_confidences else 0.0,
                "dominant_pattern_label": _dominant_pattern,
            },
        }
        if join_assessments:
            entry["join_assessments"] = join_assessments
        if sql_pairs_for_ast_diff:
            entry["_sql_pairs_for_ast_diff"] = sql_pairs_for_ast_diff[:5]
        _failure_features = _summarize_feature_diffs(feature_diffs)
        if _failure_features:
            entry["failure_features"] = _failure_features
        clusters.append(entry)

    clusters.sort(key=lambda c: len(c["question_ids"]), reverse=True)

    # ── 8b. Cluster Formation Summary ────────────────────────────────────
    if _cluster_debug and clusters:
        total_failures = sum(len(p["failures"]) for p in question_profiles.values())
        total_judges = len({f["judge"] for p in question_profiles.values() for f in p["failures"]})
        lines = [f"\n== CLUSTER FORMATION ({_pass_tag}) ============================================="]
        lines.append(f"|  Total failure entries:     {total_failures} (across {len(question_profiles)} questions, {total_judges} judges)")
        lines.append(f"|  Question profiles:         {len(question_profiles)}")
        lines.append(f"|  Cluster groups formed:     {len(clusters)}")
        if _qid_rewrites or _qid_pure_duplicates:
            lines.append(
                f"|  Duplicate qids detected:   "
                f"rewrote {len(_qid_rewrites)}, dropped {len(_qid_pure_duplicates)}"
            )
            if _qid_rewrites:
                _preview = ", ".join(_qid_rewrites[:5])
                _suffix = "" if len(_qid_rewrites) <= 5 else f" (+{len(_qid_rewrites) - 5} more)"
                lines.append(f"|    rewrites: {_preview}{_suffix}")
            if _qid_pure_duplicates:
                _dup_counts: dict[str, int] = {}
                for _q in _qid_pure_duplicates:
                    _dup_counts[_q] = _dup_counts.get(_q, 0) + 1
                _preview = ", ".join(
                    f"{q} (x{c})" for q, c in list(_dup_counts.items())[:5]
                )
                _suffix = (
                    ""
                    if len(_dup_counts) <= 5
                    else f" (+{len(_dup_counts) - 5} more)"
                )
                lines.append(f"|    pure duplicates: {_preview}{_suffix}")
        for c in clusters:
            cid = c["cluster_id"]
            rc = c["root_cause"]
            blame = c.get("asi_blame_set") or "(none)"
            qids = c["question_ids"]
            lines.append(f"|    {cid} ({rc}, blame=\"{blame}\"): {len(qids)} question(s) {qids}")
        lines.append("=" * 78)
        print("\n".join(lines))

    return clusters


# ═══════════════════════════════════════════════════════════════════════
# 2a. Cluster Priority Scoring (adaptive lever loop)
# ═══════════════════════════════════════════════════════════════════════


# Task 7: NL_TEXT judges that fail on natural-language quality alone.
# A cluster whose ``dominant_failed_judges`` is a subset of this set
# cannot be fixed by any SQL-shape lever — routing it through the
# default strategist track wastes the SQL-shape patch budget on
# narrative-only failures. ``route_nl_text_only_cluster`` returns
# ``"narrative_only"`` for these clusters; the harness then restricts
# them to instruction-section patches (lever 3) and excludes them
# from the ``MAX_AG_PATCHES`` SQL-shape budget.
NL_TEXT_ONLY_JUDGES: frozenset[str] = frozenset({
    "response_quality",
    "table_routing_quality_text",
})


def is_nl_text_only_cluster(cluster: dict) -> bool:
    """True when every failing judge in the cluster is NL_TEXT-only.

    Checks both ``dominant_failed_judges`` (set during cluster build)
    and ``affected_judges`` (legacy field) so old call sites continue
    to work.
    """
    failed_judges: set[str] = set()
    for key in ("dominant_failed_judges", "affected_judges", "failed_judges"):
        val = cluster.get(key)
        if isinstance(val, (list, tuple, set, frozenset)):
            for j in val:
                if isinstance(j, str) and j:
                    failed_judges.add(j)
    if not failed_judges:
        return False
    return failed_judges.issubset(NL_TEXT_ONLY_JUDGES)


def route_nl_text_only_cluster(cluster: dict) -> str:
    """Return the proposal track this cluster should follow.

    ``"narrative_only"`` clusters are restricted to ``add_instruction``
    / ``rewrite_instruction`` proposals (lever 3); ``"default"`` is
    the standard SQL-shape track.
    """
    if not is_nl_text_only_cluster(cluster):
        return "default"
    return "narrative_only"


# Task 7: deterministic ranking tiebreakers. Higher rank => earlier in
# the strategist's priority list. SQL-shape beats ROUTING beats
# NL_TEXT beats META so an SQL-shape cluster with the same impact
# always wins.
_SIGNAL_CLASS_RANK: dict[str, int] = {
    "sql_shape": 3,
    "routing": 2,
    "nl_text": 1,
    "meta": 0,
    "infra": 0,
    "mixed": 2,  # mixed treated mid-rank
}


def _signal_class_rank(cluster: dict) -> int:
    """Map the cluster's aggregate signal class string to a numeric
    tiebreaker value (higher beats lower)."""
    sc = cluster.get("signal_class")
    if sc is None:
        # Fall back to deriving from failing judges if signal_class
        # was not pre-stamped.
        from genie_space_optimizer.optimization.judge_classes import (
            aggregate_cluster_signal_class,
        )
        judges = []
        for key in ("affected_judges", "dominant_failed_judges", "failed_judges"):
            val = cluster.get(key)
            if isinstance(val, (list, tuple, set, frozenset)):
                judges.extend(j for j in val if isinstance(j, str))
        sc = aggregate_cluster_signal_class(judges)
    if hasattr(sc, "value"):
        sc = sc.value
    sc_str = str(sc).lower()
    return _SIGNAL_CLASS_RANK.get(sc_str, 0)


def cluster_impact(cluster: dict) -> float:
    """Score a failure cluster by estimated optimisation impact.

    ``impact = question_count × causal_weight × severity × fixability × signal_quality_dampen``

    Higher is more impactful.  Used to rank clusters before the adaptive
    strategist call so the LLM receives a suggested priority order.

    Task 11 (lever-loop improvement plan): the 0.5× soft-cluster
    damping multiplier was retired. With Task 6 stamping typed
    ``SqlDiff`` on every confirmed failure and Task 7's ranking
    tiebreaker preferring SQL_SHAPE > NL_TEXT and ``hard > soft`` at
    equal impact, damping no longer earns its keep — it just
    creates a class of clusters that can never win the rank without
    external help. Soft-cluster precedence over hard at equal raw
    impact is now expressed in :func:`rank_clusters` tiebreakers,
    not in the impact score itself, so impact scores are directly
    comparable across signal classes.

    The legacy ``reelevated`` field is kept as a no-op pass-through
    so reflection-buffer entries from older runs still deserialize
    cleanly; it has no behavioral effect after Task 11.

    T1.3 signal-quality dampen is retained: clusters built mostly
    from heuristic sql_diff attribution (no trace ASI, no fetched
    results) are still less trusted, and a 0.6–1.0× linear interp
    over ``signal_quality.combined`` keeps low-confidence clusters
    in the ranking without letting them dominate a trusted same-size
    cluster.
    """
    from genie_space_optimizer.common.config import (
        CAUSAL_WEIGHT,
        FIXABILITY_WITH_COUNTERFACTUAL,
        FIXABILITY_WITHOUT_COUNTERFACTUAL,
        SEVERITY_WEIGHT,
    )

    q_count = max(len(cluster.get("question_ids", [])), 1)
    judge = cluster.get("affected_judge", "")
    failure_type = cluster.get("asi_failure_type") or cluster.get("root_cause", "other")

    causal = CAUSAL_WEIGHT.get(judge, 1.0)
    severity = SEVERITY_WEIGHT.get(failure_type, 0.5)

    has_cf = bool(cluster.get("asi_counterfactual_fixes"))
    fixability = FIXABILITY_WITH_COUNTERFACTUAL if has_cf else FIXABILITY_WITHOUT_COUNTERFACTUAL

    # T1.3: signal-quality dampen retained — see docstring.
    _sq = cluster.get("signal_quality") or {}
    _combined = float(_sq.get("combined", 1.0))
    signal_quality_dampen = 0.6 + 0.4 * max(0.0, min(1.0, _combined))

    return q_count * causal * severity * fixability * signal_quality_dampen


_RANK_TIEBREAK_THRESHOLD = 1.0
"""Impact-score delta under which the IQ-scan tiebreaker is allowed to kick in.

When two clusters are within 1.0 of each other in ``impact_score`` we treat
the primary ordering as a tie and consult ``recommended_levers`` to choose.
Anything larger is a clear winner and the scan may not override."""


def rank_clusters(
    clusters: list[dict],
    recommended_levers: set[int] | frozenset[int] | None = None,
    reflection_buffer: list[dict] | None = None,
) -> list[dict]:
    """Return *clusters* sorted by :func:`cluster_impact` (descending).

    Each cluster dict gets ``impact_score`` and ``rank`` keys added. The
    original list is **not** mutated.

    When ``recommended_levers`` is provided (typically from the IQ Scan
    preflight) it acts as a tiebreaker: clusters within
    :data:`_RANK_TIEBREAK_THRESHOLD` of each other in ``impact_score`` are
    reordered so the one whose implied lever is in ``recommended_levers``
    wins. Clusters separated by more than the threshold are never reordered,
    so the scan strictly breaks ties and never overrides a clear impact
    winner.

    T2.1: when ``reflection_buffer`` is supplied, each cluster gains a
    ``history`` dict with ``{first_seen_iter, last_seen_iter, attempts,
    prior_outcomes}`` derived from reflection entries whose
    ``source_cluster_signatures`` contains this cluster's
    ``cluster_signature``. Strategist prompt builders downstream include
    this so the LLM can reason about "this cluster has been tried N
    times, rolled back N times with lever set X".
    """
    from genie_space_optimizer.common.config import (
        SOFT_CLUSTER_REELEVATION_THRESHOLD,
    )

    # T2.1: index reflection buffer by cluster signature up-front so we
    # don't re-scan it per cluster.
    _history_by_signature: dict[str, list[dict]] = {}
    if reflection_buffer:
        for _entry in reflection_buffer:
            for _sig in _entry.get("source_cluster_signatures") or []:
                _history_by_signature.setdefault(_sig, []).append(_entry)

    scored: list[dict] = []
    for c in clusters:
        enriched = dict(c)
        # T2.1: attach history derived from prior reflection entries.
        _sig = enriched.get("cluster_signature")
        if _sig and _sig in _history_by_signature:
            _attempts = _history_by_signature[_sig]
            _iters = sorted({a.get("iteration", -1) for a in _attempts if a.get("iteration") is not None})
            enriched["history"] = {
                "first_seen_iter": _iters[0] if _iters else None,
                "last_seen_iter": _iters[-1] if _iters else None,
                "attempts": len(_attempts),
                "rolled_back_count": sum(1 for a in _attempts if not a.get("accepted")),
                "prior_outcomes": [
                    {
                        "iteration": a.get("iteration"),
                        "accepted": bool(a.get("accepted")),
                        "lever_set": a.get("lever_set") or [],
                        "accuracy_delta": a.get("accuracy_delta", 0.0),
                        "rollback_reason": a.get("rollback_reason"),
                    }
                    for a in _attempts[-5:]  # cap the prompt footprint
                ],
            }
        # T1.12: re-elevate soft clusters whose mean judge-failure ratio
        # crosses the threshold BEFORE scoring so ``cluster_impact``
        # skips the 0.5 dampen. Hard clusters are unaffected.
        if enriched.get("signal_type") == "soft":
            _jfr = float(enriched.get("mean_judge_failure_ratio", 0.0) or 0.0)
            if _jfr >= SOFT_CLUSTER_REELEVATION_THRESHOLD:
                enriched["reelevated"] = True
                logger.info(
                    "Soft cluster re-elevated [%s] root_cause=%s "
                    "mean_judge_failure_ratio=%.3f >= threshold=%.3f "
                    "(soft dampening skipped)",
                    enriched.get("cluster_id", "?"),
                    enriched.get("root_cause", "?"),
                    _jfr,
                    SOFT_CLUSTER_REELEVATION_THRESHOLD,
                )
        enriched["impact_score"] = cluster_impact(enriched)
        if recommended_levers:
            implied_lever = _map_to_lever(
                enriched.get("root_cause", "other"),
                asi_failure_type=enriched.get("asi_failure_type"),
                blame_set=enriched.get("asi_blame_set"),
                judge=enriched.get("affected_judge"),
            )
            enriched["_scan_lever_overlap"] = (
                1.0 if implied_lever in recommended_levers else 0.0
            )
        scored.append(enriched)

    # Task 7: deterministic tiebreakers. Without these, two clusters
    # with the same impact_score (the retail-run condition where the
    # top 5 all came in at impact=1.7) sorted by Python's stable-sort
    # default — i.e. by insertion order, which tracks lexicographic
    # qid. The new key prefers (in order): impact, hard > soft, then
    # SQL_SHAPE > ROUTING > NL_TEXT > META, then signal_quality.combined,
    # then mean_judge_failure_ratio. All real signal-based; no qid
    # ordering creeps in.
    scored.sort(
        key=lambda c: (
            float(c.get("impact_score") or 0.0),
            1 if c.get("signal_type") == "hard" else 0,
            _signal_class_rank(c),
            float((c.get("signal_quality") or {}).get("combined", 0.0)),
            float(c.get("mean_judge_failure_ratio") or 0.0),
        ),
        reverse=True,
    )

    if recommended_levers:
        # Swap adjacent pairs that are within the tiebreak threshold when the
        # lower-impact cluster matches a scan-recommended lever and the higher
        # one doesn't. One left-to-right pass is sufficient: the scan never
        # fires across non-adjacent boundaries, and a single pass preserves
        # the stable impact-score ordering for all other pairs.
        i = 0
        while i < len(scored) - 1:
            hi, lo = scored[i], scored[i + 1]
            if (
                abs(hi["impact_score"] - lo["impact_score"]) <= _RANK_TIEBREAK_THRESHOLD
                and lo.get("_scan_lever_overlap", 0.0) > hi.get("_scan_lever_overlap", 0.0)
            ):
                scored[i], scored[i + 1] = lo, hi
                i += 2
            else:
                i += 1

    for i, c in enumerate(scored, 1):
        c["rank"] = i
    return scored


def format_reflection_buffer(
    reflection_buffer: list[dict],
    full_window: int | None = None,
) -> str:
    """Render the reflection buffer as a prompt-ready string.

    The most recent *full_window* entries are shown in full detail.  Older
    entries are compressed to a single line each.  A DO NOT RETRY block is
    appended at the end listing every target that was previously tried and
    rolled back.
    """
    from genie_space_optimizer.common.config import REFLECTION_WINDOW_FULL

    if full_window is None:
        full_window = REFLECTION_WINDOW_FULL

    if not reflection_buffer:
        return "(No prior iterations. This is the first attempt after baseline evaluation.)"

    lines: list[str] = []
    do_not_retry: list[str] = []
    cutoff = max(0, len(reflection_buffer) - full_window)

    for entry in reflection_buffer[:cutoff]:
        status = "ACCEPTED" if entry.get("accepted") else "ROLLED_BACK"
        action = entry.get("action", "?")[:100]
        delta = entry.get("accuracy_delta", 0.0)
        lines.append(
            f"Iter {entry.get('iteration', '?')}: {action} "
            f"({status}, accuracy delta {delta:+.1f}%)"
        )
        if not entry.get("accepted"):
            do_not_retry.extend(entry.get("do_not_retry", []))

    for entry in reflection_buffer[cutoff:]:
        status = "ACCEPTED" if entry.get("accepted") else "ROLLED_BACK"
        lines.append(f"\nITERATION {entry.get('iteration', '?')} | {status}")
        lines.append(f"  Action: {entry.get('action', '?')}")
        levers = entry.get("levers", [])
        if levers:
            lines.append(f"  Levers: {', '.join(str(l) for l in levers)}")
        targets = entry.get("target_objects", [])
        if targets:
            lines.append(f"  Targets: {', '.join(targets[:10])}")
        deltas = entry.get("score_deltas", {})
        if deltas:
            delta_parts = [f"{k} {v:+.1f}%" for k, v in sorted(deltas.items()) if v != 0]
            if delta_parts:
                lines.append(f"  Score changes: {', '.join(delta_parts)}")
        new_failures = entry.get("new_failures")
        if new_failures:
            lines.append(f"  New failures: {new_failures}")
        if entry.get("rollback_reason"):
            lines.append(f"  Rollback reason: {entry['rollback_reason']}")
        _ref_text = entry.get("reflection_text", "")
        if _ref_text:
            lines.append(f"  Reflection: {_ref_text}")
        _ref_mode = entry.get("refinement_mode", "")
        if _ref_mode and not entry.get("accepted"):
            lines.append(f"  Refinement guidance: {_ref_mode}")
        if not entry.get("accepted"):
            do_not_retry.extend(entry.get("do_not_retry", []))

    if do_not_retry:
        lines.append("\nDO NOT RETRY:")
        for item in sorted(set(do_not_retry)):
            lines.append(f"  - {item}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════
# 2b. UC Type Enrichment & Join Discovery (Lever 4)
# ═══════════════════════════════════════════════════════════════════════


def enrich_metadata_with_uc_types(
    metadata_snapshot: dict,
    uc_columns: list[dict],
) -> None:
    """Merge UC ``data_type`` and ``comment`` into metadata_snapshot column_configs.

    Mutates *metadata_snapshot* in place.  Each UC row is matched by
    ``table_name`` (unqualified) + ``column_name`` against the tables in
    ``data_sources.tables[].column_configs``.  If a column_config already has
    ``data_type`` set it is left unchanged.
    """
    if not uc_columns:
        return

    uc_lookup: dict[tuple[str, str], dict] = {}
    for row in uc_columns:
        if not isinstance(row, dict):
            continue
        tbl = str(row.get("table_name") or row.get("table") or "").strip().lower()
        col = str(row.get("column_name") or row.get("column") or "").strip().lower()
        if tbl and col:
            uc_lookup[(tbl, col)] = row
        cat = str(row.get("catalog_name") or "").strip().lower()
        sch = str(row.get("schema_name") or "").strip().lower()
        if cat and sch and tbl and col:
            uc_lookup[(f"{cat}.{sch}.{tbl}", col)] = row

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    enriched = 0
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "") or tbl.get("name", "")
        short = ident.rsplit(".", 1)[-1].lower() if ident else ""
        fqn_lower = ident.lower() if ident else ""
        for cc in tbl.get("column_configs", tbl.get("columns", [])):
            if not isinstance(cc, dict):
                continue
            col_name = (cc.get("column_name") or cc.get("name", "")).lower()
            uc_row = uc_lookup.get((fqn_lower, col_name)) or uc_lookup.get((short, col_name))
            if uc_row is None:
                continue
            if not cc.get("data_type"):
                dt = uc_row.get("data_type") or uc_row.get("type") or ""
                if dt:
                    cc["data_type"] = str(dt).upper()
                    enriched += 1
            if not cc.get("uc_comment"):
                comment = uc_row.get("comment") or ""
                if comment:
                    cc["uc_comment"] = str(comment)
    logger.info("UC type enrichment: updated %d column_configs", enriched)


# ---------------------------------------------------------------------------
# Proactive Description Enrichment
# ---------------------------------------------------------------------------

_ENRICHMENT_BATCH_THRESHOLD = 30
_MIN_DESCRIPTION_LENGTH = 10

# F7 — LLM output-budget invariants. ``_ENRICHMENT_BATCH_THRESHOLD``
# gates *whether* we batch-split (30 rows total → one call); once
# splitting kicks in, rows are grouped by table. Without a further
# cap a single very wide table can collapse into one oversized batch
# — we saw 88 blank columns for one metric view blow the 4096 output
# budget, producing an empty HTTP 200 body that downstream JSON
# parsing couldn't recover from.
#
# These caps are LLM-budget-driven invariants, not operator tunables:
# they keep the JSON output comfortably under 4096 completion tokens
# with the current enrichment prompt. If the prompt or the
# ``max_tokens`` budget changes, revisit both values together.
_MAX_COLUMNS_PER_BATCH = 25
_MAX_TABLES_PER_BATCH = 15


def _chunk_enrichment_batches(
    batches: list[list[dict]], max_size: int,
) -> list[list[dict]]:
    """Split each batch that exceeds ``max_size`` into fixed-size chunks.

    Order-preserving; already-small batches pass through untouched so
    existing test fixtures with tiny inputs see no behavioural change.
    Used by both column-level (``_MAX_COLUMNS_PER_BATCH``) and
    table-level (``_MAX_TABLES_PER_BATCH``) enrichment.

    Invariants:
    * Every output chunk has length in ``(0, max_size]``.
    * Table affinity of the input is preserved — this function never
      merges rows from different source batches, only sub-divides.
    * ``max_size <= 0`` is a caller bug and the input is returned
      unchanged (defensive; keeps enrichment running even if a future
      refactor passes a bad constant).
    """
    if max_size <= 0:
        return batches
    out: list[list[dict]] = []
    for batch in batches:
        if len(batch) <= max_size:
            out.append(batch)
            continue
        for i in range(0, len(batch), max_size):
            out.append(batch[i : i + max_size])
    return out


def _is_description_insufficient(desc: Any) -> bool:
    """Return True when a description is too short to be useful (< 10 chars)."""
    if desc is None:
        return True
    if isinstance(desc, list):
        text = " ".join(str(d).strip() for d in desc)
    else:
        text = str(desc).strip()
    return len(text) < _MIN_DESCRIPTION_LENGTH


def _collect_blank_columns(
    metadata_snapshot: dict,
) -> list[dict]:
    """Scan metadata_snapshot for columns with insufficient descriptions.

    A column is eligible when both the Genie Space description and the UC
    comment are shorter than ``_MIN_DESCRIPTION_LENGTH`` characters.

    Returns a list of dicts with keys: table, column, data_type, entity_type,
    table_description, sibling_columns.
    """
    from genie_space_optimizer.optimization.structured_metadata import (
        entity_type_for_column,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])

    blanks: list[dict] = []

    for tbl in list(tables) + list(mvs):
        if not isinstance(tbl, dict):
            continue
        identifier = tbl.get("identifier", "") or tbl.get("name", "")
        tbl_desc = tbl.get("description", [])
        if isinstance(tbl_desc, list):
            tbl_desc = "\n".join(str(d) for d in tbl_desc)
        else:
            tbl_desc = str(tbl_desc or "")

        is_mv = tbl in (mvs if isinstance(mvs, list) else [])
        cols = tbl.get("column_configs", tbl.get("columns", []))
        sibling_names = [
            cc.get("column_name", cc.get("name", ""))
            for cc in cols if isinstance(cc, dict)
        ]

        for cc in cols:
            if not isinstance(cc, dict):
                continue
            if cc.get("hidden"):
                continue
            col_name = cc.get("column_name", cc.get("name", ""))
            desc = cc.get("description")
            uc_comment = cc.get("uc_comment", "")

            if not _is_description_insufficient(desc):
                continue
            if uc_comment and len(str(uc_comment).strip()) >= _MIN_DESCRIPTION_LENGTH:
                continue

            data_type = cc.get("data_type", "")
            etype = entity_type_for_column(
                col_name, data_type,
                is_in_metric_view=is_mv,
                enable_entity_matching=bool(cc.get("enable_entity_matching")),
            )
            blanks.append({
                "table": identifier,
                "column": col_name,
                "data_type": data_type,
                "entity_type": etype,
                "table_description": tbl_desc,
                "sibling_columns": sibling_names,
            })

    return blanks


def _lookup_table_profile(data_profile: dict, table_fqn: str) -> dict:
    """Find a table's profile entry by FQN or leaf name."""
    if not data_profile:
        return {}
    hit = data_profile.get(table_fqn) or data_profile.get(table_fqn.lower())
    if hit:
        return hit
    leaf = table_fqn.split(".")[-1].strip("`").lower()
    for key, val in data_profile.items():
        if key.split(".")[-1].strip("`").lower() == leaf:
            return val
    return {}


def _format_enrichment_context(
    blanks: list[dict],
    data_profile: dict | None = None,
) -> str:
    """Format blank columns into a context string grouped by table."""
    by_table: dict[str, list[dict]] = {}
    for b in blanks:
        by_table.setdefault(b["table"], []).append(b)

    lines: list[str] = []
    for tbl_id, cols in by_table.items():
        tbl_desc = cols[0].get("table_description", "") or "(no table description)"
        siblings = cols[0].get("sibling_columns", [])
        target_names = {c["column"] for c in cols}
        sibling_context = [s for s in siblings if s not in target_names]

        tbl_profile = _lookup_table_profile(data_profile or {}, tbl_id)
        row_count = tbl_profile.get("row_count")
        tbl_header = f"Table: {tbl_id} ({tbl_desc[:200]})"
        if row_count is not None and row_count >= 0:
            tbl_header += f" [~{row_count} rows]"
        lines.append(tbl_header)
        lines.append("  Columns needing descriptions:")
        for c in cols:
            col_line = f"    - {c['column']} ({c['data_type'] or 'UNKNOWN'}) [{c['entity_type']}]"
            col_info = tbl_profile.get("columns", {}).get(c["column"], {})
            if col_info:
                hints: list[str] = []
                if col_info.get("cardinality"):
                    hints.append(f"cardinality={col_info['cardinality']}")
                if col_info.get("distinct_values"):
                    vals = col_info["distinct_values"][:10]
                    hints.append(f"values={vals}")
                if col_info.get("min") is not None:
                    hints.append(f"range=[{col_info['min']}, {col_info['max']}]")
                if hints:
                    col_line += f" — {', '.join(hints)}"
            lines.append(col_line)
        if sibling_context:
            lines.append(f"  Sibling columns (for context): {', '.join(sibling_context[:20])}")
        lines.append("")

    return "\n".join(lines)


def _format_data_profile_for_prompt(data_profile: dict | None) -> str:
    """Render data profile as a compact string for enrichment prompt templates."""
    if not data_profile:
        return "(no data profile available)"
    lines: list[str] = []
    for table, tinfo in sorted(data_profile.items()):
        row_count = tinfo.get("row_count", "?")
        lines.append(f"### {table} (~{row_count} rows)")
        for col, cinfo in sorted(tinfo.get("columns", {}).items()):
            card = cinfo.get("cardinality", "?")
            vals = cinfo.get("distinct_values")
            minv = cinfo.get("min")
            maxv = cinfo.get("max")
            parts = [f"cardinality={card}"]
            if vals:
                parts.append(f"values={vals}")
            if minv is not None:
                parts.append(f"range=[{minv}, {maxv}]")
            lines.append(f"  - {col}: {', '.join(parts)}")
    return "\n".join(lines)


def _select_metric_view_columns(*sources: dict | None) -> list[dict]:
    """Pick the first non-empty ``metric_view_columns`` payload from any
    candidate dict. Used by every column proposal builder so the
    metric-view-aware fallback in :func:`proposal_shape._normalise_one`
    can resolve MV-backed columns. Each entry must be a dict with
    ``column_name`` plus a metric-view target key (``metric_view_full_name``
    or ``metric_view``); other shapes are filtered out so a malformed
    upstream payload fails closed."""
    # NOTE: this helper is dormant until ASI cluster enrichment populates
    # ``metric_view_columns`` on the cluster / action_group / col_entry
    # payload. The shape contract is stable so the upstream enrichment task
    # can land independently without re-touching every builder.
    for src in sources:
        if not isinstance(src, dict):
            continue
        raw = src.get("metric_view_columns")
        if not raw:
            continue
        cleaned: list[dict] = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            col = str(entry.get("column_name") or "").strip()
            tgt = str(
                entry.get("metric_view_full_name")
                or entry.get("metric_view")
                or ""
            ).strip()
            if col and tgt:
                cleaned.append(entry)
        if cleaned:
            return cleaned
    return []


def _enrich_blank_descriptions(
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
    data_profile: dict | None = None,
) -> list[dict]:
    """Generate structured descriptions for columns that have no description anywhere.

    Returns a list of patch dicts compatible with the Lever 1/2 proposal format.
    Only targets columns where BOTH the Genie Space description AND the UC
    comment are empty and the column is not hidden.
    """
    from genie_space_optimizer.optimization.evaluation import _extract_json

    blanks = _collect_blank_columns(metadata_snapshot)
    if not blanks:
        logger.info("Description enrichment: 0 columns need enrichment — skipping")
        return []

    logger.info(
        "Description enrichment: %d columns with blank descriptions across %d tables",
        len(blanks),
        len({b["table"] for b in blanks}),
    )

    allowlist = _build_identifier_allowlist(metadata_snapshot)
    allowlist_str = _format_identifier_allowlist(allowlist)
    profile_context_str = _format_data_profile_for_prompt(data_profile)

    if len(blanks) <= _ENRICHMENT_BATCH_THRESHOLD:
        batches = [blanks]
    else:
        by_table: dict[str, list[dict]] = {}
        for b in blanks:
            by_table.setdefault(b["table"], []).append(b)
        batches = list(by_table.values())

    # F7 — sub-chunk any oversized per-table batch. Keeps table affinity
    # (each chunk still contains rows from only one table) so the
    # prompt's ``table_description`` / sibling context remains coherent.
    batches = _chunk_enrichment_batches(batches, _MAX_COLUMNS_PER_BATCH)

    all_patches: list[dict] = []
    system_msg = "You generate structured column descriptions for a Databricks Genie Space."

    for batch_idx, batch in enumerate(batches):
        context_str = _format_enrichment_context(batch, data_profile=data_profile)
        format_kwargs: dict[str, Any] = {
            "columns_context": context_str,
            "identifier_allowlist": allowlist_str,
            "data_profile_context": profile_context_str,
        }
        format_kwargs = _truncate_to_budget(
            format_kwargs, DESCRIPTION_ENRICHMENT_PROMPT,
            priority_keys=["columns_context"],
        )
        prompt = format_mlflow_template(DESCRIPTION_ENRICHMENT_PROMPT, **format_kwargs)

        text = ""
        try:
            text, _response = _traced_llm_call(
                w, system_msg, prompt,
                span_name=f"enrich_column_descriptions_batch_{batch_idx}",
                max_tokens=4096,
                response_validator=_extract_json,
            )
            result = _extract_json(text)
            if isinstance(result, list):
                result = {"changes": result}
        except Exception as exc:
            # F6 — prefer the last-seen HTTP 200 body stamped on the
            # exception by ``_traced_llm_call``. Falls back to local
            # ``text`` (still "" here because _traced_llm_call raised
            # before returning) if the attribute is missing.
            preview_text = getattr(exc, "last_response_text", "") or text
            chars = getattr(exc, "last_response_chars", len(preview_text))
            preview = preview_text[:300].replace("\n", "\\n")
            logger.warning(
                "Description enrichment: batch %d (table=%s, %d cols) — "
                "LLM response not parseable as JSON after retries: %s | "
                "response_chars=%d | preview=%r",
                batch_idx,
                batch[0]["table"] if batch else "?",
                len(batch),
                exc,
                chars,
                preview,
            )
            continue

        batch_lookup = {(b["table"], b["column"]): b for b in batch}

        for change in result.get("changes", []):
            tbl = change.get("table", "")
            col = change.get("column", "")
            sections = change.get("sections", {})
            etype = change.get("entity_type", "")

            if not tbl or not col or not sections:
                continue
            if (tbl, col) not in batch_lookup:
                logger.debug(
                    "Description enrichment: skipping %s.%s — not in eligible set", tbl, col,
                )
                continue

            if not etype:
                etype = batch_lookup[(tbl, col)]["entity_type"]

            all_patches.append({
                "type": "update_column_description",
                "table": tbl,
                "column": col,
                "structured_sections": sections,
                "column_entity_type": etype,
                "lever": 0,
                "risk_level": "low",
                "source": "proactive_enrichment",
            })

    logger.info(
        "Description enrichment: generated %d patches for %d blank columns",
        len(all_patches), len(blanks),
    )
    return all_patches


# ---------------------------------------------------------------------------
# Proactive Table Description Enrichment
# ---------------------------------------------------------------------------


def _collect_insufficient_tables(
    metadata_snapshot: dict,
) -> list[dict]:
    """Scan metadata_snapshot for tables with insufficient top-level descriptions.

    A table is eligible when its description is shorter than
    ``_MIN_DESCRIPTION_LENGTH`` characters.

    Returns a list of dicts with keys: table, current_description,
    column_names, column_types, is_metric_view.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])

    insufficient: list[dict] = []

    for tbl_list, is_mv in [(tables, False), (mvs, True)]:
        for tbl in tbl_list:
            if not isinstance(tbl, dict):
                continue
            identifier = tbl.get("identifier", "") or tbl.get("name", "")
            raw_desc = tbl.get("description", "")
            if isinstance(raw_desc, list):
                desc_text = "\n".join(str(d) for d in raw_desc).strip()
            else:
                desc_text = str(raw_desc or "").strip()

            if len(desc_text) >= _MIN_DESCRIPTION_LENGTH:
                continue

            cols = tbl.get("column_configs", tbl.get("columns", []))
            col_info = []
            for cc in cols:
                if not isinstance(cc, dict):
                    continue
                col_info.append({
                    "name": cc.get("column_name", cc.get("name", "")),
                    "data_type": cc.get("data_type", ""),
                })

            insufficient.append({
                "table": identifier,
                "current_description": desc_text,
                "column_names": [c["name"] for c in col_info],
                "column_types": {c["name"]: c["data_type"] for c in col_info},
                "is_metric_view": is_mv,
            })

    return insufficient


def _format_table_enrichment_context(
    tables: list[dict],
    data_profile: dict | None = None,
) -> str:
    """Format insufficient tables into a context string for the LLM prompt."""
    lines: list[str] = []
    for t in tables:
        cur = t.get("current_description", "")
        desc_label = f"({cur[:80]})" if cur else "(none)"
        tbl_profile = _lookup_table_profile(data_profile or {}, t["table"])
        row_count = tbl_profile.get("row_count")
        lines.append(f"Table: {t['table']}")
        lines.append(f"  Current description: {desc_label}")
        if row_count is not None and row_count >= 0:
            lines.append(f"  Row count: ~{row_count}")
        col_parts = []
        for cname in t.get("column_names", [])[:30]:
            ctype = t.get("column_types", {}).get(cname, "")
            col_parts.append(f"{cname} ({ctype})" if ctype else cname)
        if col_parts:
            lines.append(f"  Columns: {', '.join(col_parts)}")
            remaining = len(t.get("column_names", [])) - 30
            if remaining > 0:
                lines.append(f"  (+{remaining} more columns)")
        if t.get("is_metric_view"):
            lines.append("  Type: Metric View")
        col_profiles = tbl_profile.get("columns", {})
        if col_profiles:
            profile_hints: list[str] = []
            for cname, cinfo in list(col_profiles.items())[:15]:
                parts: list[str] = []
                if cinfo.get("distinct_values"):
                    vals = cinfo["distinct_values"][:8]
                    parts.append(f"values={vals}")
                elif cinfo.get("min") is not None:
                    parts.append(f"range=[{cinfo['min']}, {cinfo['max']}]")
                if parts:
                    profile_hints.append(f"{cname}: {', '.join(parts)}")
            if profile_hints:
                lines.append(f"  Data profile: {'; '.join(profile_hints)}")
        lines.append("")
    return "\n".join(lines)


def _enrich_table_descriptions(
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
    data_profile: dict | None = None,
) -> list[dict]:
    """Generate structured descriptions for tables that have insufficient descriptions.

    Returns a list of patch dicts compatible with ``update_description``
    proposals (lever 0, scope ``genie_config``).
    """
    from genie_space_optimizer.common.config import TABLE_DESCRIPTION_ENRICHMENT_PROMPT
    from genie_space_optimizer.optimization.evaluation import _extract_json

    tables = _collect_insufficient_tables(metadata_snapshot)
    if not tables:
        logger.info("Table description enrichment: 0 tables need enrichment — skipping")
        return []

    logger.info(
        "Table description enrichment: %d tables with insufficient descriptions",
        len(tables),
    )

    allowlist = _build_identifier_allowlist(metadata_snapshot)
    allowlist_str = _format_identifier_allowlist(allowlist)
    profile_context_str = _format_data_profile_for_prompt(data_profile)

    if len(tables) <= _ENRICHMENT_BATCH_THRESHOLD:
        batches = [tables]
    else:
        batches = [[t] for t in tables]

    # F7 — cap per-batch table count. Table prompts are denser than
    # column prompts (each table expands to a table-description block),
    # so the ceiling is lower (15 vs 25). The above-threshold path
    # already produces single-table batches, so the cap only bites
    # when ``len(tables) <= _ENRICHMENT_BATCH_THRESHOLD`` but still
    # large enough to overflow the output budget.
    batches = _chunk_enrichment_batches(batches, _MAX_TABLES_PER_BATCH)

    all_patches: list[dict] = []
    system_msg = "You generate structured table descriptions for a Databricks Genie Space."

    for batch_idx, batch in enumerate(batches):
        context_str = _format_table_enrichment_context(batch, data_profile=data_profile)
        format_kwargs: dict[str, Any] = {
            "tables_context": context_str,
            "identifier_allowlist": allowlist_str,
            "data_profile_context": profile_context_str,
        }
        format_kwargs = _truncate_to_budget(
            format_kwargs, TABLE_DESCRIPTION_ENRICHMENT_PROMPT,
            priority_keys=["tables_context"],
        )
        prompt = format_mlflow_template(TABLE_DESCRIPTION_ENRICHMENT_PROMPT, **format_kwargs)

        text = ""
        try:
            text, _response = _traced_llm_call(
                w, system_msg, prompt,
                span_name=f"enrich_table_descriptions_batch_{batch_idx}",
                max_tokens=4096,
                response_validator=_extract_json,
            )
            result = _extract_json(text)
            if isinstance(result, list):
                result = {"changes": result}
        except Exception as exc:
            # F6 — same pattern as _enrich_blank_descriptions: pull the
            # last-seen body off the exception so the warning can
            # surface a real preview instead of "".
            preview_text = getattr(exc, "last_response_text", "") or text
            chars = getattr(exc, "last_response_chars", len(preview_text))
            preview = preview_text[:300].replace("\n", "\\n")
            logger.warning(
                "Table description enrichment: batch %d (%d tables) — "
                "LLM response not parseable as JSON after retries: %s | "
                "response_chars=%d | preview=%r",
                batch_idx,
                len(batch),
                exc,
                chars,
                preview,
            )
            continue

        batch_lookup = {t["table"]: t for t in batch}

        for change in result.get("changes", []):
            tbl = change.get("table", "")
            sections = change.get("sections", {})

            if not tbl or not sections:
                continue
            if tbl not in batch_lookup:
                logger.debug(
                    "Table description enrichment: skipping %s — not in eligible set", tbl,
                )
                continue

            entity_type = "mv_table" if batch_lookup[tbl].get("is_metric_view") else "table"

            all_patches.append({
                "type": "update_description",
                "table": tbl,
                "structured_sections": sections,
                "table_entity_type": entity_type,
                "lever": 0,
                "risk_level": "low",
                "source": "proactive_enrichment",
            })

    logger.info(
        "Table description enrichment: generated %d patches for %d insufficient tables",
        len(all_patches), len(tables),
    )
    return all_patches


# ── Proactive Space Metadata Generation ──────────────────────────────


def _build_space_schema_context(metadata_snapshot: dict) -> dict[str, str]:
    """Build context strings for tables, metric views, and instructions."""
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", [])
    mvs = ds.get("metric_views", [])

    def _str_field(val: object) -> str:
        if isinstance(val, list):
            return " ".join(str(s) for s in val)
        return str(val) if val else ""

    table_lines: list[str] = []
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "")
        desc = _str_field(tbl.get("description", ""))
        cols = tbl.get("column_configs", tbl.get("columns", []))
        col_names = [
            c.get("column_name", c.get("name", ""))
            for c in cols if isinstance(c, dict)
        ]
        line = f"- {ident}"
        if desc:
            line += f": {desc[:120]}"
        if col_names:
            line += f"\n  Columns: {', '.join(col_names[:20])}"
            if len(col_names) > 20:
                line += f" (+{len(col_names) - 20} more)"
        table_lines.append(line)

    mv_lines: list[str] = []
    for mv in mvs:
        if not isinstance(mv, dict):
            continue
        ident = mv.get("identifier", "")
        desc = _str_field(mv.get("description", ""))
        cols = mv.get("column_configs", mv.get("columns", []))
        col_names = [
            c.get("column_name", c.get("name", ""))
            for c in cols if isinstance(c, dict)
        ]
        line = f"- {ident}"
        if desc:
            line += f": {desc[:120]}"
        if col_names:
            line += f"\n  Columns: {', '.join(col_names[:15])}"
        mv_lines.append(line)

    instr = metadata_snapshot.get("instructions", {})
    ti_list = instr.get("text_instructions", []) if isinstance(instr, dict) else []
    instr_parts: list[str] = []
    for ti in ti_list:
        if not isinstance(ti, dict):
            continue
        raw = ti.get("content", "")
        if isinstance(raw, list):
            raw = "\n".join(str(s) for s in raw)
        if raw:
            instr_parts.append(str(raw)[:200])
    instr_text = "\n".join(instr_parts) or "(none)"

    return {
        "tables_context": "\n".join(table_lines) or "(none)",
        "metric_views_context": "\n".join(mv_lines) or "(none)",
        "instructions_context": instr_text,
    }


def _generate_space_description(
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> str:
    """Generate a structured description for a Genie Space from its schema.

    Returns the description text, or ``""`` on failure.
    """
    ctx = _build_space_schema_context(metadata_snapshot)
    format_kwargs = _truncate_to_budget(
        ctx, SPACE_DESCRIPTION_PROMPT,
        priority_keys=["tables_context"],
    )
    prompt = format_mlflow_template(SPACE_DESCRIPTION_PROMPT, **format_kwargs)
    system_msg = "You generate structured descriptions for Databricks Genie Spaces."

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name="generate_space_description",
            max_tokens=2048,
        )
        text = re.sub(r"```[a-z]*\n?", "", text).strip().rstrip("`")
        if len(text) < 30:
            logger.warning("Space description generation: result too short (%d chars)", len(text))
            return ""
        logger.info("Space description generation: produced %d chars", len(text))
        return text
    except Exception:
        logger.warning("Space description generation: LLM call failed", exc_info=True)
        return ""


def _generate_proactive_instructions(
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
    *,
    _repair_attempt: int = 0,
    _prior_errors: list[str] | None = None,
) -> str:
    """Generate canonical 5-section routing instructions for an empty space.

    Returns the instruction text (validated against the 5-section schema)
    or ``""`` on failure. The caller is responsible for persisting the
    text via ``_set_general_instructions``.

    Self-contained repair loop (Task C.1): on validation failure, the
    function recurses ONCE with ``_repair_attempt=1`` and the specific
    errors passed back to the LLM via the prompt prefix. The harness
    doesn't need to know — ``_generate_proactive_instructions(metadata, w)``
    still returns ``""`` on total failure.

    Parameters ``_repair_attempt`` and ``_prior_errors`` are private —
    external callers should leave them at defaults.
    """
    from genie_space_optimizer.common.config import (
        MAX_TEXT_INSTRUCTIONS_CHARS, PROACTIVE_INSTRUCTION_PROMPT,
    )
    from genie_space_optimizer.optimization.applier import validate_instruction_text

    ctx = _build_space_schema_context(metadata_snapshot)

    join_specs = []
    ds = metadata_snapshot.get("data_sources", {})
    if isinstance(ds, dict):
        for tbl in ds.get("tables", []):
            if isinstance(tbl, dict):
                for js in tbl.get("join_specs", []):
                    if isinstance(js, dict):
                        sql_parts = js.get("sql", [])
                        cond = sql_parts[0] if sql_parts else ""
                        if cond:
                            join_specs.append(cond)
    ctx["join_specs_context"] = "\n".join(f"- {j}" for j in join_specs) if join_specs else "(none)"

    format_kwargs = _truncate_to_budget(
        ctx, PROACTIVE_INSTRUCTION_PROMPT,
        priority_keys=["tables_context"],
    )
    prompt = format_mlflow_template(PROACTIVE_INSTRUCTION_PROMPT, **format_kwargs)

    # Repair preamble — prepended when the first attempt failed validation.
    if _prior_errors:
        prompt = (
            "REPAIR CALL — your previous reply failed validation with these "
            "specific errors:\n"
            + "\n".join(f"  - {e}" for e in _prior_errors[:5])
            + "\n\nFix ONLY those issues in your regenerated output. "
            "Keep all other content. Return only the instruction text.\n\n"
            + prompt
        )
    system_msg = "You generate routing instructions for Databricks Genie Spaces."

    span_name = (
        "generate_proactive_instructions_repair"
        if _repair_attempt > 0
        else "generate_proactive_instructions"
    )
    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name=span_name,
            max_tokens=2048,
        )
        text = re.sub(r"```[a-z]*\n?", "", text).strip().rstrip("`")
        if len(text) < 50:
            logger.warning(
                "Proactive instruction generation: result too short (%d chars)",
                len(text),
            )
            return ""
        if len(text) > MAX_TEXT_INSTRUCTIONS_CHARS:
            # Cap at the scanner threshold — never write content that
            # would immediately fail IQ check #4. Split on a line boundary
            # so we don't truncate mid-bullet.
            text = text[:MAX_TEXT_INSTRUCTIONS_CHARS].rsplit("\n", 1)[0]
        ok, errors = validate_instruction_text(text, strict=True)
        if ok:
            logger.info(
                "Proactive instruction generation: produced %d chars%s",
                len(text),
                " (after repair)" if _repair_attempt > 0 else "",
            )
            return text

        # Validation failed — attempt one repair round.
        if _repair_attempt == 0:
            logger.info(
                "Proactive instruction generation: validation failed — "
                "retrying once with repair prompt. errors=%s",
                errors,
            )
            return _generate_proactive_instructions(
                metadata_snapshot, w,
                _repair_attempt=1, _prior_errors=errors,
            )

        logger.warning(
            "Proactive instruction generation: validation failed after "
            "repair — giving up. errors=%s sample=%r",
            errors, text[:200],
        )
        return ""
    except Exception:
        logger.warning(
            "Proactive instruction generation: LLM call failed", exc_info=True,
        )
        return ""


def _expand_instructions(
    metadata_snapshot: dict,
    existing_instructions: str,
    missing_sections: list[str],
    w: WorkspaceClient | None = None,
    *,
    _repair_attempt: int = 0,
    _prior_errors: list[str] | None = None,
) -> dict[str, str]:
    """Generate content for the canonical sections a space is missing.

    Called by the two-phase proactive seeding path (Task B.5) when a
    space already has instructions but lacks one or more of the five
    canonical sections. The LLM returns a dict keyed by exact canonical
    header; we validate every key is in ``missing_sections`` and in
    :data:`CANONICAL_SECTION_HEADERS` before returning.

    Self-contained repair loop (Task C.1): on per-section validation
    failure (SQL-in-prose, over-budget), the function recurses ONCE with
    ``_repair_attempt=1`` and the offending details passed back to the
    LLM via the prompt prefix.

    Parameters
    ----------
    metadata_snapshot
        Genie Space config snapshot (tables, MVs, join specs).
    existing_instructions
        Current prose — used by the prompt to avoid duplicating content.
    missing_sections
        Subset of canonical headers to populate. If empty, returns ``{}``.
    w
        WorkspaceClient for the LLM call (optional; pass-through).

    Returns
    -------
    dict[str, str]
        ``{canonical_header: section_body_text}`` for each missing section
        the LLM chose to populate. Sections the LLM declined to fill
        (because there was nothing meaningful to say) are absent from
        the dict — the caller merges only what was returned, never
        overwriting existing content. May also return a
        ``{"__skip_reason__": ...}`` sentinel when budget is too small
        to call the LLM.
    """
    from genie_space_optimizer.common.config import (
        CANONICAL_SECTION_HEADERS, EXPAND_INSTRUCTION_PROMPT,
        MAX_TEXT_INSTRUCTIONS_CHARS, MIN_EXPAND_BUDGET,
        sql_in_text_findings,
    )
    from genie_space_optimizer.optimization.evaluation import _extract_json

    if not missing_sections:
        return {}
    missing = [s for s in missing_sections if s in CANONICAL_SECTION_HEADERS]
    if not missing:
        return {}

    # ── Budget math (floor-free; strict upper bound on per-section budget) ──
    # Invariant: per_section_budget * missing_count <= remaining_budget
    # so the LLM can never legitimately return content that overflows. If
    # remaining room is too small to be useful, skip the LLM call entirely
    # rather than forcing a decline-after-generation round-trip.
    existing_length = len(existing_instructions or "")
    remaining_budget = max(MAX_TEXT_INSTRUCTIONS_CHARS - existing_length, 0)

    if remaining_budget < MIN_EXPAND_BUDGET:
        logger.info(
            "Expand skipped: remaining_budget=%d < MIN_EXPAND_BUDGET=%d "
            "(existing prose is near the 2000-char cap)",
            remaining_budget, MIN_EXPAND_BUDGET,
        )
        # Sentinel key so callers can distinguish "nothing to do" from
        # "wanted to do it but ran out of room" in their decline-log output.
        return {"__skip_reason__": "no_budget"}

    missing_count = len(missing)
    # Integer division is the strict upper bound. Do NOT floor — floors
    # inflate the claimed remaining room past the actual cap and defeat
    # the whole point of the pre-render trim.
    per_section_budget = remaining_budget // missing_count

    ctx = _build_space_schema_context(metadata_snapshot)

    join_specs: list[str] = []
    ds = metadata_snapshot.get("data_sources", {})
    if isinstance(ds, dict):
        for tbl in ds.get("tables", []):
            if isinstance(tbl, dict):
                for js in tbl.get("join_specs", []):
                    if isinstance(js, dict):
                        sql_parts = js.get("sql", [])
                        cond = sql_parts[0] if sql_parts else ""
                        if cond:
                            join_specs.append(cond)
    ctx["join_specs_context"] = (
        "\n".join(f"- {j}" for j in join_specs) if join_specs else "(none)"
    )
    ctx["existing_instructions"] = existing_instructions or "(none)"
    ctx["missing_sections"] = "\n".join(f"- {h}" for h in missing)
    ctx["existing_length"] = str(existing_length)
    ctx["remaining_budget"] = str(remaining_budget)
    ctx["missing_count"] = str(missing_count)
    ctx["per_section_budget"] = str(per_section_budget)

    format_kwargs = _truncate_to_budget(
        ctx, EXPAND_INSTRUCTION_PROMPT,
        priority_keys=["existing_instructions", "tables_context"],
    )
    prompt = format_mlflow_template(EXPAND_INSTRUCTION_PROMPT, **format_kwargs)

    # Repair preamble — prepended on the second attempt with the specific
    # errors from the first. Same pattern as _generate_proactive_instructions.
    if _prior_errors:
        prompt = (
            "REPAIR CALL — your previous reply failed validation with these "
            "specific errors:\n"
            + "\n".join(f"  - {e}" for e in _prior_errors[:5])
            + "\n\nFix ONLY those issues. Keep all other content. "
            "Return only the JSON.\n\n"
            + prompt
        )
    system_msg = (
        "You expand Databricks Genie Space instructions — only the "
        "missing canonical sections."
    )

    span_name = (
        "expand_instructions_repair" if _repair_attempt > 0 else "expand_instructions"
    )
    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name=span_name,
            max_tokens=1536,
        )
    except Exception:
        logger.warning("Expand instructions: LLM call failed", exc_info=True)
        return {}

    try:
        parsed = _extract_json(text)
    except (json.JSONDecodeError, ValueError):
        logger.warning(
            "Expand instructions: JSON parse failed. raw=%r", (text or "")[:200],
        )
        return {}

    if not isinstance(parsed, dict):
        logger.warning(
            "Expand instructions: expected JSON object, got %s",
            type(parsed).__name__,
        )
        return {}

    sections_raw = parsed.get("sections", parsed)
    if not isinstance(sections_raw, dict):
        return {}

    allowed = set(missing)
    out: dict[str, str] = {}
    for header, body in sections_raw.items():
        header_str = str(header).strip()
        if header_str not in allowed:
            logger.info(
                "Expand instructions: dropping non-requested / non-canonical header %r",
                header_str,
            )
            continue
        body_str: str
        if isinstance(body, list):
            body_str = "\n".join(str(s).rstrip() for s in body if str(s).strip())
        else:
            body_str = str(body).strip()
        if not body_str:
            continue
        out[header_str] = body_str

    # ── Per-section validation (repair-loop trigger) ────────────────
    # The merged-text validation happens in the harness; here we only
    # check each section INDIVIDUALLY for SQL-in-prose and per-section
    # over-budget. Those are the two failure modes the LLM can self-heal
    # via one repair call — other failures (e.g. totally empty output)
    # aren't worth a retry.
    errors: list[str] = []
    for header, body_str in out.items():
        sql_offenders = sql_in_text_findings(body_str)
        if sql_offenders:
            errors.append(
                f"Section {header!r} contains SQL: "
                f"{sql_offenders[0].strip()[:100]!r}. "
                "Rephrase using English verbs (combine, link, pair) — "
                "never SQL keywords."
            )
        if len(body_str) > per_section_budget * 2:
            # Allow 2× per-section budget headroom before triggering repair.
            # Layer 1 (harness pre-render trim) will hard-clip whatever lands,
            # but spending a repair round on a 10× overshoot produces better
            # output than trimming mid-bullet.
            errors.append(
                f"Section {header!r} is {len(body_str)} chars; "
                f"budget was ~{per_section_budget}. Trim to fit."
            )

    if errors and _repair_attempt == 0:
        logger.info(
            "Expand instructions: per-section validation failed — "
            "retrying once with repair prompt. errors=%s",
            errors,
        )
        return _expand_instructions(
            metadata_snapshot, existing_instructions, missing_sections, w=w,
            _repair_attempt=1, _prior_errors=errors,
        )

    if errors:
        logger.warning(
            "Expand instructions: validation failed after repair — "
            "dropping offending sections. errors=%s",
            errors,
        )
        # Drop any offending section rather than returning it — the harness
        # can still merge the clean ones.
        out = {h: b for h, b in out.items() if not sql_in_text_findings(b)}

    logger.info(
        "Expand instructions: produced %d/%d requested sections (%s)%s",
        len(out), len(missing), ", ".join(out.keys()) or "(none)",
        " (after repair)" if _repair_attempt > 0 else "",
    )
    return out


def _generate_sample_questions(
    metadata_snapshot: dict,
    description: str = "",
    w: WorkspaceClient | None = None,
) -> list[dict]:
    """Generate sample questions for a Genie Space from its schema.

    Returns a list of ``{"id": "<hex>", "question": ["<text>"]}`` dicts,
    or ``[]`` on failure.
    """
    from genie_space_optimizer.common.genie_schema import generate_genie_id
    from genie_space_optimizer.optimization.evaluation import _extract_json

    ctx = _build_space_schema_context(metadata_snapshot)
    ctx["description_context"] = description or "(none)"
    format_kwargs = _truncate_to_budget(
        ctx, SAMPLE_QUESTIONS_PROMPT,
        priority_keys=["tables_context"],
    )
    prompt = format_mlflow_template(SAMPLE_QUESTIONS_PROMPT, **format_kwargs)
    system_msg = "You generate sample questions for Databricks Genie Spaces."

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name="generate_sample_questions",
            max_tokens=2048,
        )
        result = _extract_json(text)
    except Exception:
        logger.warning("Sample question generation: LLM call failed", exc_info=True)
        return []

    questions = result.get("questions", [])
    if not isinstance(questions, list) or not questions:
        logger.warning("Sample question generation: no questions in LLM response")
        return []

    sample_questions: list[dict] = []
    for q in questions:
        if not isinstance(q, str) or not q.strip():
            continue
        sample_questions.append({
            "id": generate_genie_id(),
            "question": [q.strip()],
        })

    logger.info("Sample question generation: produced %d questions", len(sample_questions))
    return sample_questions


_JOIN_KEY_SUFFIXES = ("_key", "_id", "_code", "_fk", "_ref", "_num", "_no", "_sk", "_pk")
_DIM_FACT_PATTERNS = ("dim_", "fact_", "bridge_", "link_")

_COMPATIBLE_TYPE_GROUPS: list[set[str]] = [
    {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "LONG", "SHORT"},
    {"FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"},
    {"STRING", "VARCHAR", "CHAR"},
    {"DATE"},
    {"TIMESTAMP", "TIMESTAMP_NTZ"},
    {"BOOLEAN"},
]


def _short_name(identifier: str) -> str:
    """Return the unqualified table name from a fully-qualified identifier."""
    return identifier.rsplit(".", 1)[-1] if "." in identifier else identifier


def _strip_suffix(col: str) -> str:
    """Strip known join-key suffixes to get the column stem."""
    lower = col.lower()
    for sfx in _JOIN_KEY_SUFFIXES:
        if lower.endswith(sfx):
            return lower[: -len(sfx)]
    return lower


def _is_fuzzy_match(col_a: str, col_b: str) -> bool:
    """Check whether two column names are fuzzy-matches.

    Returns True when:
    - One full name is a substring of the other
    - They share the same stem after stripping join-key suffixes
    - One *stem* is a substring of the other stem (handles ``prod`` vs ``product``)
    """
    la, lb = col_a.lower(), col_b.lower()
    if la == lb:
        return True
    if la in lb or lb in la:
        return True
    stem_a = _strip_suffix(la)
    stem_b = _strip_suffix(lb)
    if not stem_a or not stem_b:
        return False
    if stem_a == stem_b:
        return True
    if stem_a in stem_b or stem_b in stem_a:
        return True
    return False


def _types_compatible(type_a: str, type_b: str) -> bool:
    """Return True if two UC data types are join-compatible.

    When either type is unknown (empty) we assume compatibility so
    that the LLM can make the final decision.
    """
    if not type_a or not type_b:
        return True
    a, b = type_a.upper().split("(")[0].strip(), type_b.upper().split("(")[0].strip()
    if a == b:
        return True
    for group in _COMPATIBLE_TYPE_GROUPS:
        if a in group and b in group:
            return True
    return False


def _extract_table_pairs_from_clusters(clusters: list[dict]) -> set[tuple[str, str]]:
    """Extract table pairs mentioned in soft signal cluster blame sets and fixes."""
    pairs: set[tuple[str, str]] = set()
    for cl in clusters:
        blame = cl.get("asi_blame_set") or cl.get("blame_set") or []
        if isinstance(blame, str):
            blame = [blame]
        fixes = cl.get("asi_counterfactual_fixes") or cl.get("counterfactual_fixes") or []
        if isinstance(fixes, str):
            fixes = [fixes]

        tables_mentioned: list[str] = []
        for item in list(blame) + list(fixes):
            item_str = str(item).lower()
            for tok in item_str.replace(",", " ").split():
                if "." in tok and len(tok.split(".")) >= 2:
                    tables_mentioned.append(tok.strip())

        for i, t1 in enumerate(tables_mentioned):
            for t2 in tables_mentioned[i + 1:]:
                if t1 != t2:
                    t_a, t_b = sorted((t1, t2))
                    pairs.add((t_a, t_b))
    return pairs


def _semantics_is_metric_view_id(metadata_snapshot: dict, identifier: str) -> bool:
    """PR 29 — Return True when ``identifier`` resolves to a metric view in
    ``metadata_snapshot["_asset_semantics"]``.

    Falls back to ``False`` (treat as non-MV) when the snapshot has no
    semantics map yet, when the identifier is empty, or when any error
    occurs during the lookup. The caller is responsible for fanning the
    legacy ``_metric_view_yaml`` cache into ``_asset_semantics`` upstream
    so this lookup is consistent for every consumer.
    """
    try:
        from genie_space_optimizer.common.asset_semantics import (
            is_metric_view as _sem_is_mv,
        )
    except Exception:
        return False
    if not identifier:
        return False
    try:
        return bool(_sem_is_mv(metadata_snapshot, identifier))
    except Exception:
        return False


def _semantics_direct_join_block_reason(
    metadata_snapshot: dict,
    identifier: str,
) -> str | None:
    """Return why direct joins are blocked for ``identifier``, if known."""
    try:
        from genie_space_optimizer.common.asset_semantics import (
            direct_join_block_reason as _block_reason,
        )
    except Exception:
        return None
    if not identifier:
        return None
    try:
        return _block_reason(metadata_snapshot, identifier)
    except Exception:
        return None


def filter_join_specs_by_semantics(
    metadata_snapshot: dict,
    join_specs: list[dict],
    *,
    counters: dict[str, int] | None = None,
    skipped_examples: list[tuple[str, str]] | None = None,
) -> list[dict]:
    """PR 29 — Drop join specs whose left or right identifier is a
    metric view per ``_asset_semantics``. Direct joins on metric views
    raise ``METRIC_VIEW_JOIN_NOT_SUPPORTED`` at execute time; gating
    them at discovery prevents the cascade entirely.

    ``counters`` is mutated in place (when supplied) with two keys:
    ``joins_skipped_metric_view_left`` and
    ``joins_skipped_metric_view_right``. ``skipped_examples`` (if
    supplied) is appended with ``(left_id, right_id)`` tuples for the
    first few skipped pairs so the call site can log a sample.
    """
    if not isinstance(join_specs, list) or not join_specs:
        return list(join_specs) if isinstance(join_specs, list) else []

    kept: list[dict] = []
    for spec in join_specs:
        if not isinstance(spec, dict):
            kept.append(spec)
            continue
        left_obj = spec.get("left") or {}
        right_obj = spec.get("right") or {}
        if isinstance(left_obj, dict):
            left_id = left_obj.get("identifier", "") or ""
        else:
            left_id = spec.get("left_table_name", "") or ""
        if isinstance(right_obj, dict):
            right_id = right_obj.get("identifier", "") or ""
        else:
            right_id = spec.get("right_table_name", "") or ""

        left_block_reason = _semantics_direct_join_block_reason(
            metadata_snapshot, left_id,
        )
        right_block_reason = _semantics_direct_join_block_reason(
            metadata_snapshot, right_id,
        )
        if left_block_reason or right_block_reason:
            if counters is not None:
                if left_block_reason:
                    key = (
                        "joins_skipped_metric_view_left"
                        if left_block_reason == "metric_view"
                        else "joins_skipped_unresolved_asset_left"
                    )
                    counters[key] = counters.get(key, 0) + 1
                if right_block_reason:
                    key = (
                        "joins_skipped_metric_view_right"
                        if right_block_reason == "metric_view"
                        else "joins_skipped_unresolved_asset_right"
                    )
                    counters[key] = counters.get(key, 0) + 1
            if skipped_examples is not None and len(skipped_examples) < 5:
                skipped_examples.append((left_id, right_id))
            continue
        kept.append(spec)
    return kept


def discover_join_candidates(
    metadata_snapshot: dict,
    soft_signal_clusters: list[dict] | None = None,
) -> list[dict]:
    """Discover potential join relationships and return **hints** for the LLM.

    Scans all table pairs for columns that look like join keys using:

    * Exact name matching on key-suffix columns
    * Fuzzy name matching (substring / shared stem)
    * Data-type compatibility filtering (when types are enriched)
    * Eval feedback from soft signal clusters (table pairs from blame sets)

    Existing join specs are excluded.  Returns a list of hint dicts
    (not final join specs) that feed into the LLM discovery prompt.

    PR 29 — Pairs whose left or right identifier resolves to
    ``kind=metric_view`` in ``metadata_snapshot["_asset_semantics"]``
    are also dropped because direct joins on a metric view raise
    ``METRIC_VIEW_JOIN_NOT_SUPPORTED``. The shared CTE-first repair in
    PR 31 still rewrites genuine MV-side queries; this filter prevents
    the LLM from ever proposing a *new* join spec that touches an MV.

    Each hint has the shape::

        {
            "left_table": str,
            "right_table": str,
            "candidate_columns": [
                {"left_col": str, "right_col": str, "reason": str}
            ],
            "type_compatible": bool,
        }
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _inst = metadata_snapshot.get("instructions", {})
    if not isinstance(_inst, dict):
        _inst = {}
    tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    join_specs = (
        metadata_snapshot.get("join_specs", [])
        or _inst.get("join_specs", [])
        or ds.get("join_specs", [])
    )

    existing_pairs: set[tuple[str, str]] = set()
    for spec in join_specs:
        if not isinstance(spec, dict):
            continue
        left_obj = spec.get("left", {})
        right_obj = spec.get("right", {})
        if isinstance(left_obj, dict) and isinstance(right_obj, dict):
            lt = left_obj.get("identifier", "")
            rt = right_obj.get("identifier", "")
        else:
            lt = spec.get("left_table_name", "")
            rt = spec.get("right_table_name", "")
        if lt and rt:
            existing_pairs.add((lt, rt))
            existing_pairs.add((rt, lt))

    # Build per-table column info: {identifier: [{name, data_type}, ...]}
    table_col_info: dict[str, list[dict[str, str]]] = {}
    for t in tables:
        if not isinstance(t, dict):
            continue
        ident = t.get("identifier", "") or t.get("name", "")
        if not ident:
            continue
        cols: list[dict[str, str]] = []
        for cc in t.get("column_configs", t.get("columns", [])):
            col = cc.get("column_name") or cc.get("name", "")
            if not col:
                continue
            dt = cc.get("data_type", "")
            cols.append({"name": col.lower(), "data_type": str(dt).upper() if dt else ""})
        if cols:
            table_col_info[ident] = cols

    # Only consider columns that look like join keys for the suffix matching
    # but also include all columns for fuzzy matching
    def _key_cols(cols: list[dict[str, str]]) -> list[dict[str, str]]:
        return [c for c in cols if any(c["name"].endswith(s) for s in _JOIN_KEY_SUFFIXES)]

    hints: list[dict] = []
    seen_pairs: set[tuple[str, str]] = set()
    idents = list(table_col_info.keys())

    for i, ident_a in enumerate(idents):
        for ident_b in idents[i + 1:]:
            if (ident_a, ident_b) in existing_pairs:
                continue

            _a, _b = sorted((ident_a, ident_b))
            pair_key = (_a, _b)
            if pair_key in seen_pairs:
                continue

            cols_a = table_col_info[ident_a]
            cols_b = table_col_info[ident_b]
            key_a = _key_cols(cols_a)
            key_b = _key_cols(cols_b)

            candidate_columns: list[dict[str, str]] = []
            all_types_compatible = True

            # 1) Exact name match on key-suffix columns
            names_b = {c["name"]: c for c in cols_b}
            for ca in key_a:
                cb = names_b.get(ca["name"])
                if cb:
                    compat = _types_compatible(ca["data_type"], cb["data_type"])
                    if not compat:
                        all_types_compatible = False
                    candidate_columns.append({
                        "left_col": ca["name"],
                        "right_col": cb["name"],
                        "reason": f"exact name match (suffix key){'' if compat else ' [TYPE MISMATCH]'}",
                    })

            # 2) Fuzzy name match on key-suffix columns (only if no exact match)
            exact_left = {c["left_col"] for c in candidate_columns}
            for ca in key_a:
                if ca["name"] in exact_left:
                    continue
                for cb in key_b:
                    if ca["name"] == cb["name"]:
                        continue
                    if _is_fuzzy_match(ca["name"], cb["name"]):
                        compat = _types_compatible(ca["data_type"], cb["data_type"])
                        if not compat:
                            all_types_compatible = False
                        candidate_columns.append({
                            "left_col": ca["name"],
                            "right_col": cb["name"],
                            "reason": f"fuzzy name match (stem/substring){'' if compat else ' [TYPE MISMATCH]'}",
                        })

            if not candidate_columns:
                continue

            seen_pairs.add(pair_key)
            hints.append({
                "left_table": ident_a,
                "right_table": ident_b,
                "candidate_columns": candidate_columns,
                "type_compatible": all_types_compatible,
            })

    # 3) Eval-feedback enrichment: soft signal clusters may reference
    # table pairs that heuristics missed (e.g., wrong join column, SCD filters).
    if soft_signal_clusters:
        feedback_pairs = _extract_table_pairs_from_clusters(soft_signal_clusters)
        ident_lower = {ident.lower(): ident for ident in table_col_info}

        for pair in feedback_pairs:
            t1_lower, t2_lower = pair
            t1_orig = ident_lower.get(t1_lower)
            t2_orig = ident_lower.get(t2_lower)
            if not t1_orig or not t2_orig:
                for k, v in ident_lower.items():
                    if t1_lower in k or k.endswith(t1_lower.rsplit(".", 1)[-1]):
                        t1_orig = t1_orig or v
                    if t2_lower in k or k.endswith(t2_lower.rsplit(".", 1)[-1]):
                        t2_orig = t2_orig or v
            if not t1_orig or not t2_orig:
                continue
            _a, _b = sorted((t1_orig, t2_orig))
            if (_a, _b) in seen_pairs or (_a, _b) in existing_pairs:
                continue

            cols_a = table_col_info.get(t1_orig, [])
            cols_b = table_col_info.get(t2_orig, [])
            names_b_map = {c["name"]: c for c in cols_b}
            cands: list[dict[str, str]] = []
            for ca in cols_a:
                cb = names_b_map.get(ca["name"])
                if cb:
                    cands.append({
                        "left_col": ca["name"],
                        "right_col": cb["name"],
                        "reason": "eval feedback: shared column name",
                    })
            if cands:
                seen_pairs.add((_a, _b))
                hints.append({
                    "left_table": t1_orig,
                    "right_table": t2_orig,
                    "candidate_columns": cands,
                    "type_compatible": True,
                    "source": "eval_feedback",
                })
        logger.info(
            "Join discovery: %d feedback pairs from %d soft signal clusters",
            len(feedback_pairs), len(soft_signal_clusters),
        )

    # Drop hint pairs whose either side is unsafe for a direct join. The
    # semantics helper returns no block reason when semantics are unavailable,
    # so this remains a no-op for snapshots that pre-date the contract.
    pre_filter = len(hints)
    skipped_left = 0
    skipped_right = 0
    skipped_unresolved_left = 0
    skipped_unresolved_right = 0
    skipped_examples: list[tuple[str, str]] = []
    filtered_hints: list[dict] = []
    for h in hints:
        lt = h.get("left_table", "") if isinstance(h, dict) else ""
        rt = h.get("right_table", "") if isinstance(h, dict) else ""
        l_reason = _semantics_direct_join_block_reason(metadata_snapshot, lt)
        r_reason = _semantics_direct_join_block_reason(metadata_snapshot, rt)
        if l_reason or r_reason:
            if l_reason == "metric_view":
                skipped_left += 1
            elif l_reason:
                skipped_unresolved_left += 1
            if r_reason == "metric_view":
                skipped_right += 1
            elif r_reason:
                skipped_unresolved_right += 1
            if len(skipped_examples) < 5:
                skipped_examples.append((lt, rt))
            continue
        filtered_hints.append(h)
    hints = filtered_hints

    if pre_filter != len(hints):
        logger.info(
            "Join discovery: dropped %d direct-join-unsafe hint pair(s) "
            "(mv_left=%d, mv_right=%d, unresolved_left=%d, unresolved_right=%d); "
            "examples=%s",
            pre_filter - len(hints), skipped_left, skipped_right,
            skipped_unresolved_left, skipped_unresolved_right,
            skipped_examples,
        )

    logger.info(
        "Join discovery: found %d hint pairs (%d existing specs)",
        len(hints), len(existing_pairs) // 2,
    )
    return hints


# ── Proactive Join Discovery (execution-proven) ──────────────────────

_FACT_PREFIXES = ("fact_", "fct_")

_JOIN_FQN_RE = re.compile(
    r"\bJOIN\s+"
    r"((?:`[^`]+`(?:\.`[^`]+`){0,2})"              # backtick-quoted 1-, 2-, or 3-part
    r"|(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*){0,2}))"   # unquoted 1-, 2-, or 3-part
    r"(?:\s+(?:AS\s+)?(\w+))?"
    r"\s*ON\s+(.+?)(?=\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|\bHAVING\b|;|\Z)",
    re.I | re.S,
)

_JOIN_USING_RE = re.compile(
    r"\bJOIN\s+"
    r"((?:`[^`]+`(?:\.`[^`]+`){0,2})"              # backtick-quoted 1-, 2-, or 3-part
    r"|(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*){0,2}))"   # unquoted 1-, 2-, or 3-part
    r"(?:\s+(?:AS\s+)?(\w+))?"
    r"\s*USING\s*\(([^)]+)\)",
    re.I | re.S,
)

_SQL_FROM_TABLE_RE = re.compile(
    r"\bFROM\s+"
    r"((?:`[^`]+`(?:\.`[^`]+`){0,2})"              # backtick-quoted 1-, 2-, or 3-part
    r"|(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*){0,2}))",  # unquoted 1-, 2-, or 3-part
    re.I,
)


def _convert_fk_to_candidates(
    fk_rows: list[dict],
    short_to_fqn: dict[str, str] | None = None,
) -> list[dict]:
    """Convert FK constraint dicts into the join candidate format.

    Each FK dict (from ``get_foreign_keys_for_tables_rest`` or its Spark
    fallback) has ``child_table``, ``child_columns``, ``parent_table``,
    ``parent_columns``, ``constraint_name``.

    Returns candidates compatible with the pipeline used by
    ``_corroborate_with_uc_metadata`` and ``_build_join_specs_from_proven``,
    with an extra ``fk_constraint: True`` flag so downstream logic can
    recognise their authoritative provenance.
    """
    candidates: list[dict] = []
    seen_pairs: set[tuple[str, str]] = set()
    for fk in fk_rows:
        child_fqn = fk.get("child_table", "")
        parent_fqn = fk.get("parent_table", "")
        child_cols = fk.get("child_columns", [])
        parent_cols = fk.get("parent_columns", [])
        if not (child_fqn and parent_fqn and child_cols and parent_cols):
            continue
        if len(child_cols) != len(parent_cols):
            continue

        child_short = _short_name(child_fqn).lower()
        parent_short = _short_name(parent_fqn).lower()

        on_parts = [
            f"`{child_short}`.`{cc}` = `{parent_short}`.`{pc}`"
            for cc, pc in zip(child_cols, parent_cols)
        ]
        on_condition = " AND ".join(on_parts)

        _pk_a, _pk_b = sorted((child_fqn, parent_fqn))
        pair_key = (_pk_a, _pk_b)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        candidates.append({
            "left_table": child_fqn,
            "right_table": parent_fqn,
            "on_condition": on_condition,
            "frequency": 0,
            "agreed": False,
            "source_questions": [],
            "fk_constraint": True,
            "constraint_name": fk.get("constraint_name", ""),
        })

    logger.info(
        "FK→candidates: converted %d FK constraints into %d join candidates",
        len(fk_rows), len(candidates),
    )
    return candidates


def _extract_proven_joins(
    rows: list[dict],
    metadata_snapshot: dict,
) -> list[dict]:
    """Extract execution-validated join paths from baseline eval rows.

    Considers rows where the arbiter verdict is positive (``both_correct``,
    ``genie_correct``, or ``ground_truth_correct``).  Parses JOIN…ON clauses
    from both Genie SQL and ground-truth SQL, resolves short table names to
    FQN identifiers, and returns deduplicated candidates sorted by frequency.
    """
    _POSITIVE_VERDICTS = {"both_correct", "genie_correct", "ground_truth_correct"}

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", [])

    short_to_fqn: dict[str, str] = {}
    _ambiguous_shorts: set[str] = set()
    for t in list(tables) + list(mvs):
        if not isinstance(t, dict):
            continue
        ident = t.get("identifier", "") or t.get("name", "")
        if ident:
            short = _short_name(ident).lower().strip("`")
            if short in short_to_fqn and short_to_fqn[short] != ident:
                _ambiguous_shorts.add(short)
            short_to_fqn[short] = ident
            fqn_lower = ident.lower().strip("`")
            short_to_fqn[fqn_lower] = ident

    logger.info(
        "Join extraction: short_to_fqn has %d entries, %d ambiguous shorts",
        len(short_to_fqn), len(_ambiguous_shorts),
    )

    candidates: dict[tuple[str, str], dict] = {}
    _diag_positive = 0
    _diag_has_join = 0
    _diag_no_from = 0
    _diag_no_resolve = 0

    for row in rows:
        arbiter = (
            row.get("arbiter/value")
            or row.get("feedback/arbiter/value")
            or row.get("arbiter")
            or ""
        )
        if str(arbiter).lower() not in _POSITIVE_VERDICTS:
            continue

        _diag_positive += 1
        qid = _row_qid(row)

        _req = row.get("request") or {}
        if isinstance(_req, str):
            try:
                _req = json.loads(_req)
            except (json.JSONDecodeError, TypeError):
                _req = {}
        _resp = row.get("response") or {}
        if isinstance(_resp, str):
            try:
                _resp = json.loads(_resp)
            except (json.JSONDecodeError, TypeError):
                _resp = {}

        gt_sql = (
            (_req.get("expected_sql", "") if isinstance(_req, dict) else "")
            or row.get("inputs/expected_sql", "")
            or ""
        )
        genie_sql = (
            (_resp.get("response", "") if isinstance(_resp, dict) else "")
            or row.get("outputs/response", "")
            or ""
        )

        for sql, source_label in [(gt_sql, "gt"), (genie_sql, "genie")]:
            if not sql:
                continue

            join_on_matches = list(_JOIN_FQN_RE.finditer(sql))
            join_using_matches = list(_JOIN_USING_RE.finditer(sql))
            if not join_on_matches and not join_using_matches:
                continue

            _diag_has_join += 1

            from_m = _SQL_FROM_TABLE_RE.search(sql)
            from_table_raw = from_m.group(1).replace("`", "").lower() if from_m else ""
            from_fqn = ""
            if from_table_raw:
                from_fqn = short_to_fqn.get(from_table_raw, "")
                if not from_fqn:
                    from_short = _short_name(from_table_raw).lower()
                    if from_short not in _ambiguous_shorts:
                        from_fqn = short_to_fqn.get(from_short, "")
                    elif "." in from_table_raw and from_table_raw.count(".") >= 2:
                        from_fqn = from_table_raw

            if not from_fqn:
                _diag_no_from += 1
                all_matches = len(join_on_matches) + len(join_using_matches)
                logger.debug(
                    "Join extraction [%s/%s]: no FROM table resolved "
                    "(raw=%r), skipping %d JOINs",
                    qid, source_label, from_table_raw, all_matches,
                )
                continue

            def _resolve_joined_table(raw: str) -> str:
                """Resolve a raw joined table name to FQN."""
                fqn = short_to_fqn.get(raw, "")
                if not fqn:
                    short = _short_name(raw).lower()
                    if short not in _ambiguous_shorts:
                        fqn = short_to_fqn.get(short, "")
                    elif "." in raw and raw.count(".") >= 2:
                        fqn = raw
                if not fqn and "." in raw and raw.count(".") >= 2:
                    fqn = raw
                return fqn

            parsed_joins: list[tuple[str, str]] = []
            for m in join_on_matches:
                joined_table_raw = m.group(1).replace("`", "").lower()
                on_clause = m.group(3).strip()
                parsed_joins.append((joined_table_raw, on_clause))

            for m in join_using_matches:
                joined_table_raw = m.group(1).replace("`", "").lower()
                using_cols = [c.strip().strip("`") for c in m.group(3).split(",")]
                from_short = _short_name(from_fqn).lower()
                joined_short = _short_name(joined_table_raw).lower() or joined_table_raw
                on_parts = [
                    f"`{from_short}`.`{c}` = `{joined_short}`.`{c}`"
                    for c in using_cols
                ]
                on_clause = " AND ".join(on_parts)
                parsed_joins.append((joined_table_raw, on_clause))

            for joined_table_raw, on_clause in parsed_joins:
                joined_fqn = _resolve_joined_table(joined_table_raw)

                if not joined_fqn:
                    _diag_no_resolve += 1
                    logger.debug(
                        "Join extraction [%s/%s]: cannot resolve "
                        "joined table %r to FQN",
                        qid, source_label, joined_table_raw,
                    )
                    continue

                _pk_l, _pk_r = sorted((from_fqn, joined_fqn))
                pair_key = (_pk_l, _pk_r)
                if pair_key[0] == pair_key[1]:
                    continue

                if pair_key not in candidates:
                    candidates[pair_key] = {
                        "left_table": pair_key[0],
                        "right_table": pair_key[1],
                        "on_conditions": {},
                        "frequency": 0,
                        "source_questions": [],
                        "from_gt": set(),
                        "from_genie": set(),
                    }

                entry = candidates[pair_key]
                entry["frequency"] += 1
                if qid not in entry["source_questions"]:
                    entry["source_questions"].append(qid)
                entry[f"from_{source_label}"].add(qid)

                on_norm = re.sub(r"\s+", " ", on_clause).strip()
                if on_norm:
                    entry["on_conditions"][on_norm] = entry["on_conditions"].get(on_norm, 0) + 1

    result: list[dict] = []
    for pair_key, entry in candidates.items():
        gt_qs = entry.pop("from_gt")
        genie_qs = entry.pop("from_genie")
        agreed_qs = gt_qs & genie_qs
        entry["agreed"] = len(agreed_qs) > 0

        best_condition = ""
        if entry["on_conditions"]:
            best_condition = max(entry["on_conditions"], key=entry["on_conditions"].get)
        entry["on_condition"] = best_condition
        del entry["on_conditions"]

        result.append(entry)

    result.sort(key=lambda x: (-int(x.get("agreed", False)), -x["frequency"]))

    diagnostics = {
        "total_rows": len(rows),
        "positive_verdicts": _diag_positive,
        "sql_with_join": _diag_has_join,
        "no_from_resolved": _diag_no_from,
        "no_joined_resolved": _diag_no_resolve,
    }

    logger.info(
        "Proactive join discovery: %d candidates from %d rows "
        "(positive_verdicts=%d, sql_with_join=%d, "
        "no_from_resolved=%d, no_joined_resolved=%d)",
        len(result), len(rows),
        _diag_positive, _diag_has_join,
        _diag_no_from, _diag_no_resolve,
    )
    for cand in result:
        logger.info(
            "  candidate: %s <-> %s  freq=%d agreed=%s on=%s",
            cand["left_table"], cand["right_table"],
            cand["frequency"], cand["agreed"],
            cand.get("on_condition", "")[:80],
        )
    return result, diagnostics


def _corroborate_with_uc_metadata(
    candidates: list[dict],
    metadata_snapshot: dict,
) -> list[dict]:
    """Filter proven join candidates by UC column type compatibility.

    Rejects candidates whose join columns have known incompatible types.
    Candidates with unknown types pass through (benefit of the doubt for
    execution-proven joins).

    Builds the type lookup using both short-name and FQN keys to avoid
    mismatches in multi-catalog environments.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    col_types: dict[tuple[str, str], str] = {}
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "") or tbl.get("name", "")
        short = _short_name(ident).lower()
        fqn_lower = ident.lower()
        for cc in tbl.get("column_configs", tbl.get("columns", [])):
            col_name = (cc.get("column_name") or cc.get("name", "")).lower()
            dt = cc.get("data_type", "")
            if col_name and dt:
                col_types[(short, col_name)] = str(dt).upper()
                col_types[(fqn_lower, col_name)] = str(dt).upper()

    validated: list[dict] = []
    for cand in candidates:
        on_cond = cand.get("on_condition", "")
        if not on_cond:
            validated.append(cand)
            continue

        pattern = r"`?(\w+)`?\s*\.\s*`?(\w+)`?\s*=\s*`?(\w+)`?\s*\.\s*`?(\w+)`?"
        match = re.search(pattern, on_cond)
        if not match:
            validated.append(cand)
            continue

        alias_l, col_l = match.group(1).lower(), match.group(2).lower()
        alias_r, col_r = match.group(3).lower(), match.group(4).lower()

        left_fqn = cand.get("left_table", "").lower()
        right_fqn = cand.get("right_table", "").lower()
        type_l = (
            col_types.get((left_fqn, col_l), "")
            or col_types.get((alias_l, col_l), "")
        )
        type_r = (
            col_types.get((right_fqn, col_r), "")
            or col_types.get((alias_r, col_r), "")
        )

        if type_l and type_r and not _types_compatible(type_l, type_r):
            logger.info(
                "Proactive join: rejecting %s <-> %s — type mismatch %s(%s) vs %s(%s)",
                cand["left_table"], cand["right_table"],
                col_l, type_l, col_r, type_r,
            )
            continue

        cand["type_compatible"] = True
        validated.append(cand)

    logger.info(
        "Proactive join: %d/%d candidates passed UC type check",
        len(validated), len(candidates),
    )
    return validated


def _build_join_specs_from_proven(
    candidates: list[dict],
    metadata_snapshot: dict,
) -> list[dict]:
    """Convert validated candidates into proper Genie API join_spec dicts.

    Assigns relationship types heuristically: fact→dim gets MANY_TO_ONE,
    everything else defaults to MANY_TO_ONE as it is the most common
    star-schema pattern.
    """
    from genie_space_optimizer.common.genie_schema import ensure_join_spec_fields
    from genie_space_optimizer.optimization.applier import _validate_join_spec_entry

    specs: list[dict] = []
    for cand in candidates:
        left_fqn = cand["left_table"]
        right_fqn = cand["right_table"]
        on_condition = cand.get("on_condition", "")

        left_short = _short_name(left_fqn).lower()
        right_short = _short_name(right_fqn).lower()

        left_is_fact = any(left_short.startswith(p) for p in _FACT_PREFIXES)
        right_is_fact = any(right_short.startswith(p) for p in _FACT_PREFIXES)

        if left_is_fact and not right_is_fact:
            rt = "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE"
        elif right_is_fact and not left_is_fact:
            left_fqn, right_fqn = right_fqn, left_fqn
            left_short, right_short = right_short, left_short
            rt = "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE"
        else:
            rt = "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE"

        sql_parts = []
        if on_condition:
            equijoin_only = _extract_equijoin_predicates(on_condition)
            if equijoin_only:
                on_condition = equijoin_only
            normalized = re.sub(
                r"`?\w+`?\.",
                lambda m: m.group(0),
                on_condition,
            )
            has_backticks = "`" in normalized
            if not has_backticks:
                normalized = re.sub(
                    r"(\w+)\.(\w+)",
                    r"`\1`.`\2`",
                    normalized,
                )
            sql_parts.append(normalized)
        sql_parts.append(f"--rt={rt}--")

        spec = {
            "left": {"identifier": left_fqn, "alias": left_short},
            "right": {"identifier": right_fqn, "alias": right_short},
            "sql": sql_parts,
        }
        spec = ensure_join_spec_fields(spec, config=metadata_snapshot)

        if not _validate_join_spec_entry(spec):
            logger.info(
                "Proactive join: spec rejected by validation — %s <-> %s",
                left_fqn, right_fqn,
            )
            continue

        valid, reason = validate_join_spec_types(spec, metadata_snapshot)
        if not valid:
            logger.info(
                "Proactive join: spec rejected by type check — %s <-> %s: %s",
                left_fqn, right_fqn, reason,
            )
            continue

        spec["_proactive_metadata"] = {
            "frequency": cand.get("frequency", 0),
            "agreed": cand.get("agreed", False),
            "source_questions": cand.get("source_questions", []),
        }
        specs.append(spec)

    specs.sort(
        key=lambda s: (
            -int(s.get("_proactive_metadata", {}).get("agreed", False)),
            -s.get("_proactive_metadata", {}).get("frequency", 0),
        )
    )

    logger.info(
        "Proactive join: built %d valid join specs from %d candidates",
        len(specs), len(candidates),
    )
    return specs


# ═══════════════════════════════════════════════════════════════════════
# 3. ASI Extraction
# ═══════════════════════════════════════════════════════════════════════


def read_asi_from_uc(
    spark: Any,
    mlflow_run_id: str,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Query ``genie_eval_asi_results`` Delta table via Spark."""
    table = f"{catalog}.{schema}.genie_eval_asi_results"
    try:
        df = spark.sql(
            f"""
            SELECT run_id, iteration, question_id, judge, value,
                   failure_type, severity, confidence, blame_set,
                   counterfactual_fix, wrong_clause, expected_value,
                   actual_value, missing_metadata, ambiguity_detected
            FROM {table}
            WHERE run_id = '{mlflow_run_id}'
            ORDER BY question_id, judge
            """
        )
        rows: list[dict] = []
        for r in df.collect():
            row_dict = r.asDict()
            if row_dict.get("blame_set"):
                try:
                    row_dict["blame_set"] = json.loads(row_dict["blame_set"])
                except (json.JSONDecodeError, TypeError):
                    row_dict["blame_set"] = [row_dict["blame_set"]]
            rows.append(row_dict)
        return rows
    except Exception:
        logger.exception("read_asi_from_uc failed")
        return []


def _extract_asi_from_assessments(assessments: list) -> list[dict]:
    """Parse ASI metadata from MLflow assessments list."""
    feedbacks: list[dict] = []
    for a in assessments:
        if not isinstance(a, dict):
            continue
        meta = a.get("metadata", {})
        if not isinstance(meta, dict):
            continue
        feedbacks.append(
            {
                "value": a.get("value", ""),
                "judge": a.get("name", ""),
                "question_id": a.get("question_id", ""),
                "failure_type": meta.get("failure_type", ""),
                "blame_set": meta.get("blame_set", []),
                "counterfactual_fix": [meta.get("counterfactual_fix", "")],
                "confidence": float(meta.get("confidence", 0.5)),
                "asi_severity": meta.get("severity", ""),
                "asi_wrong_clause": meta.get("wrong_clause", ""),
                "asi_expected_value": meta.get("expected_value", ""),
                "asi_actual_value": meta.get("actual_value", ""),
                "asi_missing_metadata": meta.get("missing_metadata", ""),
                "asi_ambiguity_detected": meta.get("ambiguity_detected", False),
            }
        )
    return feedbacks


def _extract_judge_feedbacks_from_eval(
    eval_results: dict,
    catalog: str = "",
    schema: str = "",
    warehouse_id: str = "",
) -> list[dict]:
    """Extract judge feedback dicts from eval results using UC-first priority chain."""
    direct = eval_results.get("judge_feedbacks") or eval_results.get("feedbacks")
    if isinstance(direct, list) and direct:
        return direct

    rows = (
        eval_results.get("eval_results")
        or eval_results.get("rows")
        or eval_results.get("table")
    )
    if not isinstance(rows, list):
        return []

    feedbacks: list[dict] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        for col, val in list(row.items()):
            if col.endswith("/value") and str(val).lower() in ("no", "false"):
                judge = col.replace("/value", "")
                meta_col = f"{judge}/metadata"
                meta = row.get(meta_col, {})
                if isinstance(meta, dict) and meta.get("failure_type"):
                    feedbacks.append(
                        {
                            "value": val,
                            "judge": judge,
                            "failure_type": meta.get("failure_type", ""),
                            "blame_set": meta.get("blame_set", []),
                            "counterfactual_fix": [meta.get("counterfactual_fix", "")],
                            "confidence": float(meta.get("confidence", 0.7)),
                            "question_id": _row_qid(row, fallback=f"q{i}"),
                            "feedback_id": f"r{i}_{judge}",
                        }
                    )
                    continue

            if col.startswith("feedback/") and "no" in str(val).lower():
                judge = col.replace("feedback/", "")
                rationale = row.get(f"rationale/{judge}", row.get("rationale", ""))
                feedbacks.append(
                    {
                        "value": val,
                        "blame_set": _infer_blame_from_rationale(rationale),
                        "counterfactual_fix": [],
                        "feedback_id": f"r{i}_{judge}",
                        "question_id": row.get(
                            "inputs/question", row.get("question", f"q{i}")
                        ),
                        "confidence": 0.7,
                    }
                )
    return feedbacks


def _infer_blame_from_rationale(rationale: str, metadata_snapshot: dict | None = None) -> list[str]:
    """Infer blame_set from judge rationale for grouping."""
    r = (rationale or "").lower()
    blame: list[str] = []
    if "table" in r:
        blame.append("tables")
    if "column" in r:
        blame.append("column_metadata")
    if "join" in r:
        blame.append("joins")
    if "filter" in r:
        blame.append("filters")
    if "instruction" in r:
        blame.append("instructions")
    return blame if blame else ["_ungrouped"]


# ═══════════════════════════════════════════════════════════════════════
# 4. LLM-powered Proposal Generation
# ═══════════════════════════════════════════════════════════════════════


def _extract_metadata_for_blame(
    metadata_snapshot: dict, blame_set: Any
) -> str:
    """Extract relevant metadata sections for the blamed objects.

    Handles fully-qualified names (``catalog.schema.table``) by matching
    on the last dotted component as well as the full string.
    """
    if not blame_set or not metadata_snapshot:
        return "(no metadata available)"

    blame_items = blame_set if isinstance(blame_set, list) else [str(blame_set)]
    blame_lower = set()
    for b in blame_items:
        bl = b.lower().strip()
        blame_lower.add(bl)
        if "." in bl:
            blame_lower.add(bl.rsplit(".", 1)[-1])

    sections: list[str] = []
    matched_tables: set[str] = set()

    for table in metadata_snapshot.get("tables", []):
        table_name = table.get("name") or table.get("identifier", "")
        tn_lower = table_name.lower()
        short_name = tn_lower.rsplit(".", 1)[-1] if "." in tn_lower else tn_lower
        if any(b in tn_lower or b == short_name for b in blame_lower):
            if table_name in matched_tables:
                continue
            matched_tables.add(table_name)
            sections.append(f"Table: {table_name}")
            for col in table.get("columns", table.get("column_configs", [])):
                col_name = col.get("name") or col.get("column_name", "")
                desc = col.get("description", "")
                if isinstance(desc, list):
                    desc = " ".join(desc)
                sections.append(f"  Column: {col_name} — {desc}")

    for b in blame_lower:
        for table in metadata_snapshot.get("tables", []):
            for col in table.get("columns", table.get("column_configs", [])):
                col_name = col.get("name") or col.get("column_name", "")
                if b == col_name.lower():
                    desc = col.get("description", "")
                    if isinstance(desc, list):
                        desc = " ".join(desc)
                    tname = table.get("name") or table.get("identifier", "")
                    sections.append(f"Column {tname}.{col_name}: {desc}")

    from genie_space_optimizer.optimization.applier import _get_general_instructions
    instructions = _get_general_instructions(metadata_snapshot)
    if instructions and any("instruction" in b for b in blame_lower):
        sections.append(f"Instructions: {instructions[:500]}")

    return "\n".join(sections) if sections else "(blamed objects not found in metadata)"


def _format_full_schema_context(
    metadata_snapshot: dict,
    filter_tables: set[str] | None = None,
) -> str:
    """Build a full schema summary of all tables, columns, descriptions, and synonyms.

    Gives the LLM complete visibility into the Genie Space structure so it can
    make informed decisions about which columns need descriptions vs. which
    should inherit from Unity Catalog, and which synonyms already exist.

    If *filter_tables* is provided, only tables whose identifier (lowercased)
    is in the set are included — useful for scoping join discovery prompts.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    from genie_space_optimizer.optimization.structured_metadata import (
        deduplicate_structured_description,
    )

    lines: list[str] = []
    for tbl in tables:
        if filter_tables is not None:
            tbl_id = (tbl.get("identifier", "") or "").lower()
            if tbl_id not in filter_tables:
                continue
        identifier = tbl.get("identifier", "")
        tbl_desc = tbl.get("description", [])
        if isinstance(tbl_desc, list):
            tbl_desc = "\n".join(tbl_desc)
        tbl_desc = deduplicate_structured_description(tbl_desc)
        lines.append(f"### Table: {identifier}")
        if tbl_desc:
            lines.append(f"  Description: {tbl_desc}")
        for cc in tbl.get("column_configs", []):
            col_name = cc.get("column_name", "")
            data_type = cc.get("data_type", "")
            desc = cc.get("description", [])
            if isinstance(desc, list):
                desc = "\n".join(desc) if desc else ""
            desc = deduplicate_structured_description(desc) if desc else ""
            uc_comment = cc.get("uc_comment", "")
            if not desc and uc_comment:
                desc = uc_comment
            syns = cc.get("synonyms", [])
            type_part = f" ({data_type})" if data_type else ""
            desc_part = f" -- {desc}" if desc else " -- (from UC)"
            syn_part = f" | synonyms: {syns}" if syns else ""
            lines.append(f"  - `{col_name}`{type_part}{desc_part}{syn_part}")
    return "\n".join(lines) if lines else "(no schema available)"


def _format_schema_index(
    metadata_snapshot: dict,
    *,
    relevant_objects: set[str] | None = None,
) -> str:
    """Compact table-of-contents for the triage strategist.

    Each table gets a single line with column names and types — no descriptions,
    no synonyms. Keeps the prompt small while giving full schema awareness.

    When ``relevant_objects`` is a non-empty set of identifiers (FQN or short
    name), only tables matching any entry's short name are included. Column-
    level FQNs (4+ dot-separated parts) are mapped to their parent table's
    short name; 3-or-fewer-part FQNs use the last segment. Empty set / None
    falls back to the full schema (backward-compatible).

    G3 (2026-05-17 lever-6 hardening plan, Task 5) — schema dump consumed
    34% of the Trial-5 lever-6 prompt budget; filtering by cluster-blame
    targets drops it to ~5% for typical 1-3 table blame sets.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    if relevant_objects:
        # Normalize relevant_objects to a set of table short names. Same
        # logic as _build_identifier_allowlist for cross-helper consistency.
        relevant_short: set[str] = set()
        for r in relevant_objects:
            if not isinstance(r, str) or not r:
                continue
            parts = r.split(".")
            if len(parts) >= 4:
                relevant_short.add(parts[-2].lower())
            else:
                relevant_short.add(parts[-1].lower())
        tables = [
            t for t in tables
            if isinstance(t, dict)
            and (t.get("identifier") or t.get("name") or "")
                .rsplit(".", 1)[-1].lower() in relevant_short
        ]

    lines: list[str] = []
    for tbl in tables:
        identifier = tbl.get("identifier", "")
        cols = tbl.get("column_configs", [])
        col_parts: list[str] = []
        for cc in cols:
            cname = cc.get("column_name", "")
            dtype = cc.get("data_type", "")
            col_parts.append(f"{cname}:{dtype}" if dtype else cname)
        col_preview = ", ".join(col_parts[:12])
        suffix = f", ... +{len(cols) - 12} more" if len(cols) > 12 else ""
        lines.append(f"- {identifier} ({len(cols)} cols: {col_preview}{suffix})")
    return "\n".join(lines) if lines else "(no schema available)"


def _build_identifier_allowlist(
    metadata_snapshot: dict,
    uc_columns: list[dict] | None = None,
    relevant_objects: set[str] | None = None,
) -> dict[str, Any]:
    """Extract an authoritative allowlist of all valid identifiers from metadata.

    Merges Genie Config (tables, metric views, functions, column_configs)
    with UC column metadata to produce a single source of truth that LLM
    prompts and static validators can reference.

    When ``relevant_objects`` is a non-empty set of fully-qualified
    identifiers, the returned allowlist is filtered to:
      - tables/MVs/functions whose FQN appears in relevant_objects, AND
      - tables that are 1-hop joined neighbors of any relevant table
        (per ``join_specs`` in the space config).
    Empty or None relevant_objects falls back to the full allowlist.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables_list = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    funcs_list = metadata_snapshot.get("functions", []) or ds.get("functions", []) or []
    mvs_list = ds.get("metric_views", []) or metadata_snapshot.get("metric_views", []) or []

    # 1-hop join neighbor expansion (only when relevant_objects given).
    if relevant_objects:
        # Column-level FQNs (4+ parts) collapse to the table's short
        # name; 3-or-fewer-part FQNs use the last segment.
        relevant_short: set[str] = set()
        for r in relevant_objects:
            if not isinstance(r, str):
                continue
            parts = r.split(".")
            if len(parts) >= 4:
                relevant_short.add(parts[-2].lower())
            else:
                relevant_short.add(parts[-1].lower())
        neighbors_short: set[str] = set()
        for tbl in tables_list:
            if not isinstance(tbl, dict):
                continue
            tbl_short = (tbl.get("identifier") or tbl.get("name") or "").rsplit(".", 1)[-1].lower()
            if tbl_short not in relevant_short:
                continue
            for js in tbl.get("join_specs", []) or []:
                if not isinstance(js, dict):
                    continue
                for cond in js.get("sql", []) or []:
                    if not isinstance(cond, str):
                        continue
                    # Naive identifier extraction: any token that looks
                    # like an existing table short name.
                    cond_lower = cond.lower()
                    for other in tables_list:
                        if not isinstance(other, dict):
                            continue
                        other_short = (other.get("identifier") or other.get("name") or "").rsplit(".", 1)[-1].lower()
                        if other_short and other_short != tbl_short and other_short in cond_lower:
                            neighbors_short.add(other_short)
        keep_short = relevant_short | neighbors_short
        tables_list = [
            t for t in tables_list
            if isinstance(t, dict)
            and (t.get("identifier") or t.get("name") or "").rsplit(".", 1)[-1].lower() in keep_short
        ]
        # Filter MVs and functions by FQN (no neighbor expansion — only
        # tables have join_specs).
        mvs_list = [
            mv for mv in mvs_list
            if (isinstance(mv, dict)
                and (mv.get("identifier") or mv.get("name") or "") in relevant_objects)
            or (isinstance(mv, str) and mv in relevant_objects)
        ]
        funcs_list = [
            fn for fn in funcs_list
            if (isinstance(fn, dict)
                and (fn.get("identifier") or fn.get("name") or "") in relevant_objects)
            or (isinstance(fn, str) and fn in relevant_objects)
        ]

    table_ids: list[str] = []
    tables_short: set[str] = set()
    columns_by_table: dict[str, list[tuple[str, str]]] = {}
    columns_flat: set[str] = set()

    uc_type_lookup: dict[tuple[str, str], str] = {}
    if uc_columns:
        for row in uc_columns:
            if not isinstance(row, dict):
                continue
            tbl = str(row.get("table_name") or "").strip().lower()
            col = str(row.get("column_name") or "").strip().lower()
            dtype = str(row.get("data_type") or "").strip().upper()
            if tbl and col:
                uc_type_lookup[(tbl, col)] = dtype

    for tbl in tables_list:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "") or tbl.get("name", "")
        if not ident:
            continue
        table_ids.append(ident)
        short = ident.rsplit(".", 1)[-1].lower()
        tables_short.add(short)

        col_entries: list[tuple[str, str]] = []
        for cc in tbl.get("column_configs", tbl.get("columns", [])):
            if not isinstance(cc, dict):
                continue
            col_name = cc.get("column_name") or cc.get("name") or ""
            dtype = cc.get("data_type") or ""
            if not dtype:
                dtype = uc_type_lookup.get((short, col_name.lower()), "")
            col_entries.append((col_name, dtype.upper() if dtype else ""))
            if col_name:
                columns_flat.add(f"{short}.{col_name}".lower())
                columns_flat.add(col_name.lower())
        columns_by_table[short] = col_entries

    func_ids: list[str] = []
    funcs_short: set[str] = set()
    for fn in funcs_list:
        if isinstance(fn, dict):
            name = fn.get("identifier", "") or fn.get("name", "")
        else:
            name = str(fn)
        if name:
            func_ids.append(name)
            funcs_short.add(name.rsplit(".", 1)[-1].lower())

    mv_ids: list[str] = []
    for mv in mvs_list:
        if isinstance(mv, dict):
            name = mv.get("identifier", "") or mv.get("name", "")
        else:
            name = str(mv)
        if name:
            mv_ids.append(name)

    return {
        "tables": table_ids,
        "tables_short": tables_short,
        "columns": columns_by_table,
        "columns_flat": columns_flat,
        "functions": func_ids,
        "functions_short": funcs_short,
        "metric_views": mv_ids,
    }


def _format_identifier_allowlist(allowlist: dict[str, Any]) -> str:
    """Render the identifier allowlist as a prompt-ready string."""
    sections: list[str] = []

    if allowlist.get("tables"):
        lines = ["VALID TABLES (use ONLY these in FROM/JOIN):"]
        for t in allowlist["tables"]:
            lines.append(f"- {t}")
        sections.append("\n".join(lines))

    if allowlist.get("columns"):
        lines = ["VALID COLUMNS BY TABLE (use ONLY these column names):"]
        for tbl_short, cols in sorted(allowlist["columns"].items()):
            if not cols:
                continue
            col_parts = []
            for col_name, dtype in cols:
                col_parts.append(f"{col_name} ({dtype})" if dtype else col_name)
            lines.append(f"{tbl_short}: {', '.join(col_parts)}")
        sections.append("\n".join(lines))

    if allowlist.get("functions"):
        lines = ["VALID FUNCTIONS (use ONLY these):"]
        for fn in allowlist["functions"]:
            lines.append(f"- {fn}")
        sections.append("\n".join(lines))

    if allowlist.get("metric_views"):
        lines = ["VALID METRIC VIEWS:"]
        for mv in allowlist["metric_views"]:
            lines.append(f"- {mv}")
        sections.append("\n".join(lines))

    return "\n\n".join(sections) if sections else "(no assets configured)"


_SQL_TABLE_REF_RE = re.compile(
    r"(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+"
    r"(`[^`]+`(?:\.`[^`]+`)*"
    r"|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)"
    r"(?:\s*\()?",
    re.IGNORECASE,
)


def _validate_sql_identifiers(
    sql: str,
    allowlist: dict[str, Any],
) -> tuple[bool, list[str]]:
    """Deterministic cross-check of SQL table/column refs against the allowlist.

    Returns ``(is_valid, violations)`` where *violations* is a list of
    human-readable strings describing each unrecognized identifier.
    This does NOT require a SparkSession — purely regex-based.
    """
    violations: list[str] = []
    if not sql or not allowlist:
        return True, violations

    tables_full = {t.lower() for t in (allowlist.get("tables") or [])}
    tables_short = {s.lower() for s in (allowlist.get("tables_short") or set())}
    cols_flat = {c.lower() for c in (allowlist.get("columns_flat") or set())}

    for mv in allowlist.get("metric_views") or []:
        mv_lower = mv.lower()
        tables_full.add(mv_lower)
        tables_short.add(mv_lower.rsplit(".", 1)[-1])

    for m in _SQL_TABLE_REF_RE.finditer(sql):
        ref = m.group(1).replace("`", "").strip()
        ref_lower = ref.lower()
        leaf = ref_lower.rsplit(".", 1)[-1]
        if ref_lower not in tables_full and leaf not in tables_short:
            violations.append(f"Unknown table: {ref}")

    sql_upper = sql.upper()
    for kw in ("SELECT", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "ON"):
        idx = sql_upper.find(kw)
        if idx < 0:
            continue
        end = len(sql)
        for stop_kw in ("FROM", "JOIN", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "UNION"):
            si = sql_upper.find(stop_kw, idx + len(kw))
            if 0 < si < end and stop_kw != kw:
                end = si
        clause = sql[idx + len(kw):end]
        for col_match in re.finditer(
            r"(?<![:\w])([A-Za-z_]\w*)\s*(?:\.([A-Za-z_]\w*))?",
            clause,
        ):
            part1 = col_match.group(1).lower()
            part2 = (col_match.group(2) or "").lower()
            if part2:
                candidate = f"{part1}.{part2}"
                if candidate in cols_flat:
                    continue
                if part2 in cols_flat:
                    continue
            else:
                if part1 in cols_flat or part1 in tables_short:
                    continue
                if part1 in _SQL_KEYWORDS:
                    continue

    return (len(violations) == 0, violations)


_SQL_KEYWORDS = frozenset({
    "select", "from", "where", "and", "or", "not", "in", "is", "null",
    "as", "on", "join", "left", "right", "inner", "outer", "cross", "full",
    "group", "by", "order", "asc", "desc", "having", "limit", "offset",
    "union", "all", "distinct", "case", "when", "then", "else", "end",
    "between", "like", "exists", "count", "sum", "avg", "min", "max",
    "cast", "coalesce", "nullif", "true", "false", "insert", "update",
    "delete", "into", "values", "set", "create", "alter", "drop", "table",
    "view", "index", "with", "recursive", "over", "partition", "row_number",
    "rank", "dense_rank", "lag", "lead", "first_value", "last_value",
    "date", "timestamp", "string", "int", "integer", "bigint", "decimal",
    "float", "double", "boolean", "array", "map", "struct", "measure",
    "current_date", "current_timestamp", "extract", "year", "month", "day",
    "hour", "minute", "second", "interval", "trim", "upper", "lower",
    "substring", "concat", "length", "replace", "round", "floor", "ceil",
    "abs", "if", "ifnull", "isnull", "nvl", "to_date", "date_format",
    "datediff", "dateadd", "months_between", "trunc", "try_cast",
})


def _format_compact_cluster_summaries(clusters: list[dict]) -> str:
    """One-liner per cluster for the triage strategist — no SQL diffs."""
    if not clusters:
        return "(No failure clusters.)"

    lines: list[str] = []
    for cluster in clusters:
        cid = cluster.get("cluster_id", "?")
        rc = cluster.get("root_cause", "unknown")
        qids = cluster.get("question_ids", [])
        blame = cluster.get("asi_blame_set")
        if isinstance(blame, str) and blame:
            blame_parts = [b.strip() for b in blame.split("|")][:5]
        elif isinstance(blame, list):
            blame_parts = [str(b) for b in blame[:5]]
        else:
            blame_parts = []
        fixes = cluster.get("asi_counterfactual_fixes", [])
        fix_str = "; ".join(str(f)[:120] for f in fixes[:2]) if fixes else ""

        parts = [f"{cid}: {rc} ({len(qids)} questions)"]
        if blame_parts:
            parts.append(f"blamed=[{', '.join(blame_parts)}]")
        if fix_str:
            parts.append(f'fixes=["{fix_str}"]')

        qtext_samples: list[str] = []
        for qt in cluster.get("question_traces", [])[:2]:
            qt_text = qt.get("question_text", "")[:100]
            if qt_text:
                qtext_samples.append(qt_text)
        for sc in cluster.get("sql_contexts", [])[:2]:
            qt_text = sc.get("question", "")[:100]
            if qt_text and qt_text not in qtext_samples:
                qtext_samples.append(qt_text)
        if qtext_samples:
            parts.append(f"sample_qs=[{'; '.join(qtext_samples[:2])}]")

        lines.append(" | ".join(parts))
    return "\n".join(lines)


def _format_structured_column_context(
    metadata_snapshot: dict,
    blame_set: Any,
    lever: int,
) -> str:
    """Build structured column metadata with editability markers for the LLM.

    Shows each relevant column's current structured sections with [EDITABLE]
    or [LOCKED] markers based on lever ownership.  Falls back to all tables
    when blame_set is empty.
    """
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        classify_column,
        entity_type_for_column,
        extract_synonyms_section,
        merge_synonyms,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", []) or []

    mv_identifiers = {
        (m.get("identifier") or "").rsplit(".", 1)[-1].lower()
        for m in mvs
    }

    blame_lower: set[str] = set()
    if blame_set:
        items = blame_set if isinstance(blame_set, list) else [str(blame_set)]
        for b in items:
            bl = b.lower().strip()
            blame_lower.add(bl)
            if "." in bl:
                blame_lower.add(bl.rsplit(".", 1)[-1])

    owned_sections = LEVER_SECTION_OWNERSHIP.get(lever, set())
    lines: list[str] = []
    columns_shown = 0
    max_columns = 40

    for tbl in tables:
        identifier = tbl.get("identifier", "")
        short_name = identifier.rsplit(".", 1)[-1].lower() if identifier else ""
        is_mv = short_name in mv_identifiers

        if blame_lower:
            tbl_match = short_name in blame_lower or identifier.lower() in blame_lower
            col_match = any(
                (cc.get("column_name") or "").lower() in blame_lower
                for cc in tbl.get("column_configs", [])
            )
            if not tbl_match and not col_match:
                continue

        lines.append(f"### Table: {identifier}")

        for cc in tbl.get("column_configs", []):
            if columns_shown >= max_columns:
                break
            col_name = cc.get("column_name", "")
            if not col_name:
                continue

            data_type = cc.get("data_type", "")
            desc = cc.get("description", [])
            syns = cc.get("synonyms", [])
            uc_comment = cc.get("uc_comment", "")

            desc_text = desc
            if isinstance(desc_text, list):
                desc_text = "\n".join(desc_text)
            if not desc_text and uc_comment:
                desc_text = uc_comment

            sections = parse_structured_description(desc_text)

            if syns:
                existing_syn = extract_synonyms_section(sections)
                all_syns = merge_synonyms(existing_syn, syns)
                from genie_space_optimizer.optimization.structured_metadata import (
                    format_synonyms_section,
                )

                sections["synonyms"] = format_synonyms_section(all_syns)

            etype = entity_type_for_column(
                col_name, data_type, is_in_metric_view=is_mv,
            )
            kind = classify_column(col_name, data_type, is_in_metric_view=is_mv)
            template_sections = ENTITY_TYPE_TEMPLATES.get(etype, [])

            lines.append(f"  Column: `{col_name}` ({data_type or 'unknown'}) [type: {kind}]")
            for sk in template_sections:
                label = SECTION_LABELS[sk]
                value = sections.get(sk, "").strip()
                marker = "[EDITABLE]" if sk in owned_sections else "[LOCKED]"
                lines.append(
                    f"    {marker} **{label}:** {value if value else '(empty)'}"
                )
            preamble = sections.get("_preamble", "").strip()
            if preamble:
                lines.append(f"    [Legacy text]: {preamble}")

            _profile = metadata_snapshot.get("_data_profile", {})
            _tbl_profile = (
                _profile.get(identifier, {})
                or _profile.get(identifier.lower(), {})
            )
            _col_profile = _tbl_profile.get("columns", {}).get(col_name, {})
            if _col_profile.get("distinct_values"):
                lines.append(f"    Data values: {_col_profile['distinct_values']}")
            elif _col_profile.get("min") is not None:
                lines.append(
                    f"    Data range: [{_col_profile['min']}, {_col_profile['max']}]"
                )

            lines.append("")
            columns_shown += 1

        if columns_shown >= max_columns:
            lines.append("  ... (additional columns omitted for brevity)")
            break

    return "\n".join(lines) if lines else "(no structured column metadata available)"


def _format_structured_table_context(
    metadata_snapshot: dict,
    blame_set: Any,
    lever: int,
) -> str:
    """Build structured table-level metadata with editability markers for the LLM.

    Shows each relevant table's current structured sections (Purpose, Best For,
    Grain, SCD, Relationships) with [EDITABLE]/[LOCKED] markers based on lever
    ownership.
    """
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", []) or []
    mv_identifiers = {
        (m.get("identifier") or "").rsplit(".", 1)[-1].lower()
        for m in mvs
    }

    blame_lower: set[str] = set()
    if blame_set:
        items = blame_set if isinstance(blame_set, list) else [str(blame_set)]
        for b in items:
            bl = b.lower().strip()
            blame_lower.add(bl)
            if "." in bl:
                blame_lower.add(bl.rsplit(".", 1)[-1])

    owned_sections = LEVER_SECTION_OWNERSHIP.get(lever, set())
    lines: list[str] = []

    for tbl in tables:
        identifier = tbl.get("identifier", "")
        short_name = identifier.rsplit(".", 1)[-1].lower() if identifier else ""
        is_mv = short_name in mv_identifiers

        if blame_lower:
            tbl_match = short_name in blame_lower or identifier.lower() in blame_lower
            col_match = any(
                (cc.get("column_name") or "").lower() in blame_lower
                for cc in tbl.get("column_configs", [])
            )
            if not tbl_match and not col_match:
                continue

        etype = "mv_table" if is_mv else "table"
        template_sections = ENTITY_TYPE_TEMPLATES.get(etype, [])

        desc = tbl.get("description", [])
        desc_text = "\n".join(desc) if isinstance(desc, list) else str(desc or "")
        sections = parse_structured_description(desc_text)

        lines.append(f"### Table: {identifier} (entity_type: {etype})")
        for sk in template_sections:
            label = SECTION_LABELS[sk]
            value = sections.get(sk, "").strip()
            marker = "[EDITABLE]" if sk in owned_sections else "[LOCKED]"
            lines.append(f"  {marker} **{label}:** {value if value else '(empty)'}")
        preamble = sections.get("_preamble", "").strip()
        if preamble:
            lines.append(f"  [Legacy text]: {preamble}")
        lines.append("")

    return "\n".join(lines) if lines else "(no structured table metadata available)"


def _format_structured_function_context(
    metadata_snapshot: dict,
    lever: int,
) -> str:
    """Build structured function metadata with editability markers for the LLM.

    Shows each function's current metadata in structured sections (Purpose,
    Best For, Use Instead Of, Parameters, Example).
    """
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])
    if not funcs:
        return "(no functions in this Genie Space)"

    owned_sections = LEVER_SECTION_OWNERSHIP.get(lever, set())
    template_sections = ENTITY_TYPE_TEMPLATES.get("function", [])
    lines: list[str] = []

    for fn in funcs:
        name = fn.get("name") or fn.get("identifier", "")
        comment = fn.get("comment") or fn.get("description") or ""
        if isinstance(comment, list):
            comment = "\n".join(comment)
        sections = parse_structured_description(comment)

        lines.append(f"### Function: {name}")
        for sk in template_sections:
            label = SECTION_LABELS[sk]
            value = sections.get(sk, "").strip()
            marker = "[EDITABLE]" if sk in owned_sections else "[LOCKED]"
            lines.append(f"  {marker} **{label}:** {value if value else '(empty)'}")
        preamble = sections.get("_preamble", "").strip()
        if preamble:
            lines.append(f"  [Legacy text]: {preamble}")
        lines.append("")

    return "\n".join(lines)


def _describe_patch_type(patch_type: str) -> str:
    """Human-readable description of a patch type."""
    pt_info = PATCH_TYPES.get(patch_type)
    if pt_info:
        return (
            f"{patch_type}: scope={pt_info['scope']}, "
            f"risk={pt_info['risk_level']}, affects={pt_info['affects']}"
        )
    return patch_type


def _format_sql_diffs(
    cluster: dict,
    *,
    max_sql_chars: int = 0,
    max_sample_chars: int = 500,
) -> str:
    """Build a human-readable summary of SQL diffs for the LLM prompt.

    When *max_sql_chars* > 0, individual SQL blocks are truncated to that
    length to keep overall prompt size manageable.  When *max_sample_chars*
    > 0, ground-truth and Genie result samples from the comparison dict are
    included (truncated to *max_sample_chars*).
    """
    sql_contexts = cluster.get("sql_contexts", [])
    if not sql_contexts:
        return "(no SQL context available)"

    def _trunc(sql: str) -> str:
        if max_sql_chars > 0 and len(sql) > max_sql_chars:
            return sql[:max_sql_chars] + " ... (truncated)"
        return sql

    lines: list[str] = []
    for idx, ctx in enumerate(sql_contexts[:3], 1):
        q = ctx.get("question", "")
        exp = _trunc(ctx.get("expected_sql", ""))
        gen = _trunc(ctx.get("generated_sql", ""))
        comp = ctx.get("comparison", {})
        lines.append(f"### Question {idx}: {q}")
        lines.append(f"**Expected SQL:**\n```sql\n{exp}\n```")
        lines.append(f"**Generated SQL:**\n```sql\n{gen}\n```")
        if isinstance(comp, dict) and comp.get("error"):
            lines.append(f"**Error:** {comp['error']}")
        elif isinstance(comp, dict) and not comp.get("match"):
            match_type = comp.get("match_type", "unknown")
            lines.append(f"**Mismatch type:** {match_type}")
        if max_sample_chars > 0 and isinstance(comp, dict):
            gt_sample = comp.get("gt_sample")
            if gt_sample:
                lines.append(
                    f"**Expected Result (sample):**\n```\n{str(gt_sample)[:max_sample_chars]}\n```"
                )
            genie_sample = comp.get("genie_sample")
            if genie_sample:
                lines.append(
                    f"**Genie Result (sample):**\n```\n{str(genie_sample)[:max_sample_chars]}\n```"
                )
        lines.append("")
    cf = cluster.get("asi_counterfactual_fixes", [])
    if cf:
        lines.append("**Suggested fixes from judges:**")
        for fix in cf[:3]:
            lines.append(f"- {fix}")
    return "\n".join(lines)


def _format_blamed_column_values(
    clusters: list[dict],
    data_profile: dict,
    *,
    max_columns: int = 20,
) -> str:
    """Build a compact value-hint section for columns that appear in blame sets.

    Extracts blamed column and table references from failure clusters, looks
    them up in *data_profile*, and renders their distinct values or ranges.
    """
    if not data_profile:
        return "(no data profile available)"

    blamed_refs: set[str] = set()
    for cluster in clusters:
        blame = cluster.get("asi_blame_set")
        if isinstance(blame, str) and blame:
            blamed_refs.update(b.strip().lower() for b in blame.split("|") if b.strip())
        elif isinstance(blame, list):
            blamed_refs.update(str(b).strip().lower() for b in blame if b)

    if not blamed_refs:
        return "(no blamed columns identified)"

    profile_lower: dict[str, dict] = {}
    for table_fqn, tinfo in data_profile.items():
        tkey = table_fqn.lower()
        short_name = tkey.split(".")[-1] if "." in tkey else tkey
        for col_name, cinfo in tinfo.get("columns", {}).items():
            col_lower = col_name.lower()
            profile_lower[f"{tkey}.{col_lower}"] = cinfo
            profile_lower[f"{short_name}.{col_lower}"] = cinfo
            profile_lower[col_lower] = cinfo

    lines: list[str] = []
    matched = 0
    for ref in sorted(blamed_refs):
        if matched >= max_columns:
            break
        cinfo = profile_lower.get(ref)
        if not cinfo:
            continue
        matched += 1
        card = cinfo.get("cardinality", "?")
        parts = [f"cardinality={card}"]
        vals = cinfo.get("distinct_values")
        if vals:
            parts.append(f"values={vals}")
        minv = cinfo.get("min")
        maxv = cinfo.get("max")
        if minv is not None:
            parts.append(f"range=[{minv}, {maxv}]")
        lines.append(f"- {ref}: {', '.join(parts)}")

    return "\n".join(lines) if lines else "(no matching column profiles found)"


def _derive_blame_from_sql(cluster: dict) -> list[str] | None:
    """Derive blamed table/column references from SQL diffs when ASI blame_set is empty."""
    sql_contexts = cluster.get("sql_contexts", [])
    if not sql_contexts:
        return None
    blamed: set[str] = set()
    for ctx in sql_contexts[:3]:
        exp_tables = _extract_sql_tables(ctx.get("expected_sql", ""))
        gen_tables = _extract_sql_tables(ctx.get("generated_sql", ""))
        blamed.update(exp_tables | gen_tables)
    return sorted(blamed)[:5] if blamed else None


def _format_existing_example_sqls(metadata_snapshot: dict) -> str:
    """Format existing example_question_sqls for inclusion in the Lever 6 prompt."""
    example_sqls = _get_existing_example_sqls(metadata_snapshot)
    if not example_sqls:
        return "(none)"
    lines: list[str] = []
    for ex in example_sqls[:20]:
        if not isinstance(ex, dict):
            continue
        q = ex.get("question", "")
        if isinstance(q, list):
            q = q[0] if q else ""
        sql = ex.get("sql", "")
        if isinstance(sql, list):
            sql = sql[0] if sql else ""
        if not q:
            continue
        entry = f"- Q: {q}\n  SQL: {sql[:200]}"
        params = ex.get("parameters", [])
        if params:
            param_strs = [
                f"{p.get('name', '?')}:{p.get('type_hint', 'STRING')}"
                for p in params if isinstance(p, dict)
            ]
            entry += f"\n  Params: {', '.join(param_strs)}"
        guidance = ex.get("usage_guidance", [])
        if guidance:
            g = guidance[0] if isinstance(guidance, list) else str(guidance)
            entry += f"\n  Guidance: {g[:150]}"
        lines.append(entry)
    return "\n".join(lines) if lines else "(none)"


def _format_eval_summary(clusters: list[dict]) -> str:
    """Produce a compact summary of all evaluation clusters for the holistic prompt."""
    if not clusters:
        return "No failure clusters from evaluation (all questions passed)."

    hard = [c for c in clusters if c.get("signal_type") != "soft"]
    soft = [c for c in clusters if c.get("signal_type") == "soft"]
    total_questions = sum(len(c.get("question_ids", [])) for c in clusters)
    root_causes: dict[str, int] = {}
    judges: dict[str, int] = {}
    for c in clusters:
        rc = c.get("root_cause", "unknown")
        root_causes[rc] = root_causes.get(rc, 0) + len(c.get("question_ids", []))
        judge = c.get("affected_judge", "unknown")
        judges[judge] = judges.get(judge, 0) + len(c.get("question_ids", []))

    lines = [
        f"Total clusters: {len(clusters)} (hard failures: {len(hard)}, soft signals: {len(soft)})",
        f"Total affected questions: {total_questions}",
        "",
        "Failures by root cause:",
    ]
    for rc, count in sorted(root_causes.items(), key=lambda x: -x[1]):
        lines.append(f"  - {rc}: {count} questions")
    lines.append("")
    lines.append("Failures by judge:")
    for judge, count in sorted(judges.items(), key=lambda x: -x[1]):
        lines.append(f"  - {judge}: {count} questions")
    return "\n".join(lines)


def _format_lever_summary(lever_changes: list[dict] | None) -> str:
    """Format what levers 1-4 did for the holistic lever 5 prompt."""
    if not lever_changes:
        return "(No changes applied by earlier levers in this iteration.)"

    lines: list[str] = []
    for lc in lever_changes:
        lever_name = lc.get("lever_name", f"Lever {lc.get('lever', '?')}")
        delta = lc.get("accuracy_delta", 0)
        patches = lc.get("patches", [])
        delta_str = f"+{delta:.1f}%" if delta >= 0 else f"{delta:.1f}%"
        lines.append(f"### {lever_name} (accuracy change: {delta_str})")
        for p in patches[:10]:
            change = p.get("change", "")
            ptype = p.get("patch_type", "")
            lines.append(f"  - [{ptype}] {change}")
        if len(patches) > 10:
            lines.append(f"  ... and {len(patches) - 10} more patches")
        lines.append("")
    return "\n".join(lines) if lines else "(No changes applied.)"


def _normalize_blame(raw: Any) -> list[str]:
    """Normalize a blame_set value to a flat list of strings."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str) and raw:
        return [b.strip() for b in raw.split("|") if b.strip()]
    return []


def _format_cluster_briefs(
    clusters: list[dict],
    top_n: int = 5,
    max_sql_chars: int = 0,
) -> str:
    """Format cluster data for the holistic prompt.

    Hard-failure clusters (top N) get SQL diffs; remaining get one-line
    summaries.  Soft-signal clusters are rendered under a separate header.

    When *max_sql_chars* > 0 each SQL block in the diff is truncated to that
    length — useful for keeping the strategist prompt within budget.
    """
    if not clusters:
        return "(No failure clusters.)"

    hard = [c for c in clusters if c.get("signal_type") != "soft"]
    soft = [c for c in clusters if c.get("signal_type") == "soft"]

    sorted_hard = sorted(hard, key=lambda c: len(c.get("question_ids", [])), reverse=True)
    lines: list[str] = []

    for idx, cluster in enumerate(sorted_hard[:top_n], 1):
        rc = cluster.get("root_cause", "unknown")
        q_ids = cluster.get("question_ids", [])
        judge = cluster.get("affected_judge", "unknown")
        blame = _normalize_blame(cluster.get("asi_blame_set"))
        cid = cluster.get("cluster_id", f"C{idx:03d}")
        lines.append(f"### {cid}: {rc} ({len(q_ids)} questions, judge: {judge})")
        if blame:
            lines.append(f"Blamed objects: {', '.join(blame[:5])}")
        lines.append(_format_sql_diffs(cluster, max_sql_chars=max_sql_chars))
        lines.append("")

    remaining_hard = sorted_hard[top_n:]
    if remaining_hard:
        lines.append(f"### Additional hard-failure clusters ({len(remaining_hard)} more):")
        for cluster in remaining_hard:
            rc = cluster.get("root_cause", "unknown")
            q_ids = cluster.get("question_ids", [])
            judge = cluster.get("affected_judge", "unknown")
            blame = _normalize_blame(cluster.get("asi_blame_set"))
            blame_str = f" blamed=[{', '.join(blame[:3])}]" if blame else ""
            lines.append(
                f"  - {rc}: {len(q_ids)} questions (judge: {judge}){blame_str}"
            )

    if soft:
        sorted_soft = sorted(soft, key=lambda c: len(c.get("question_ids", [])), reverse=True)
        lines.append("")
        lines.append("### Correct-but-Suboptimal Patterns (arbiter: correct, individual judges: failed)")
        lines.append("These queries returned correct results but used suboptimal approaches.")
        lines.append("Use these to inform best-practice guidance, NOT to fix failures.")
        lines.append("")
        for idx, cluster in enumerate(sorted_soft[:top_n], 1):
            rc = cluster.get("root_cause", "unknown")
            q_ids = cluster.get("question_ids", [])
            judge = cluster.get("affected_judge", "unknown")
            blame = _normalize_blame(cluster.get("asi_blame_set"))
            cid = cluster.get("cluster_id", f"S{idx:03d}")
            lines.append(f"#### {cid}: {rc} ({len(q_ids)} questions, judge: {judge})")
            if blame:
                lines.append(f"Blamed objects: {', '.join(blame[:5])}")
            lines.append(_format_sql_diffs(cluster, max_sql_chars=max_sql_chars))
            lines.append("")
        remaining_soft = sorted_soft[top_n:]
        if remaining_soft:
            lines.append(f"#### Additional soft-signal clusters ({len(remaining_soft)} more):")
            for cluster in remaining_soft:
                rc = cluster.get("root_cause", "unknown")
                q_ids = cluster.get("question_ids", [])
                judge = cluster.get("affected_judge", "unknown")
                blame = _normalize_blame(cluster.get("asi_blame_set"))
                blame_str = f" blamed=[{', '.join(blame[:3])}]" if blame else ""
                lines.append(
                    f"  - {rc}: {len(q_ids)} questions (judge: {judge}){blame_str}"
                )

    return "\n".join(lines)


def _format_cluster_briefs_afs(clusters: list[dict], top_n: int = 5) -> str:
    """AFS-projected variant of :func:`_format_cluster_briefs`.

    Bug #4 (P2.2) — every cluster is projected through ``format_afs``
    before rendering so no raw benchmark text (question, expected_sql,
    generated_sql, sample rows) reaches the strategist/holistic prompt.
    The legacy ``_format_cluster_briefs`` is retained for debug logging
    behind ``GSO_DEBUG_RAW_SQL=1`` only.
    """
    if not clusters:
        return "(No failure clusters.)"

    from genie_space_optimizer.optimization.afs import format_afs

    hard = [c for c in clusters if c.get("signal_type") != "soft"]
    soft = [c for c in clusters if c.get("signal_type") == "soft"]
    sorted_hard = sorted(
        hard, key=lambda c: len(c.get("question_ids", [])), reverse=True,
    )

    lines: list[str] = []
    for idx, cluster in enumerate(sorted_hard[:top_n], 1):
        afs = format_afs(cluster)
        cid = afs.get("cluster_id", f"C{idx:03d}")
        ft = afs.get("failure_type", "unknown")
        qc = afs.get("question_count", 0)
        judge = afs.get("affected_judge", "unknown")
        lines.append(f"### {cid}: {ft} ({qc} questions, judge: {judge})")
        blame = afs.get("blame_set") or []
        if blame:
            lines.append(f"Blamed objects: {', '.join(blame[:5])}")
        qids = afs.get("question_ids") or []
        if qids:
            preview = qids[:10]
            suffix = f", +{len(qids) - 10} more" if len(qids) > 10 else ""
            lines.append(f"Question IDs: {', '.join(preview)}{suffix}")
        cf = afs.get("counterfactual_fixes") or []
        if cf:
            lines.append("Suggested fixes:")
            for f in cf[:3]:
                lines.append(f"  - {f}")
        sd = afs.get("structural_diff") or {}
        if sd:
            lines.append(f"Structural signature: {json.dumps(sd, default=str)}")
        ff = afs.get("failure_features") or {}
        if ff:
            lines.append(f"Typed failure features: {json.dumps(ff, default=str)}")
        vp = afs.get("judge_verdict_pattern")
        if vp:
            lines.append(f"Judge verdict pattern: {vp}")
        lines.append("")

    remaining = sorted_hard[top_n:]
    if remaining:
        lines.append(f"### Additional hard-failure clusters ({len(remaining)} more):")
        for cluster in remaining:
            afs = format_afs(cluster)
            blame = afs.get("blame_set") or []
            blame_str = f" blamed=[{', '.join(blame[:3])}]" if blame else ""
            lines.append(
                f"  - {afs.get('failure_type', 'unknown')}: "
                f"{afs.get('question_count', 0)} questions "
                f"(judge: {afs.get('affected_judge', 'unknown')}){blame_str}"
            )

    if soft:
        sorted_soft = sorted(
            soft, key=lambda c: len(c.get("question_ids", [])), reverse=True,
        )
        lines.append("")
        lines.append("### Correct-but-Suboptimal Patterns (arbiter: correct, individual judges: failed)")
        lines.append("These queries returned correct results but used suboptimal approaches.")
        lines.append("Use these to inform best-practice guidance, NOT to fix failures.")
        lines.append("")
        for idx, cluster in enumerate(sorted_soft[:top_n], 1):
            afs = format_afs(cluster)
            cid = afs.get("cluster_id", f"S{idx:03d}")
            lines.append(
                f"#### {cid}: {afs.get('failure_type', 'unknown')} "
                f"({afs.get('question_count', 0)} questions, "
                f"judge: {afs.get('affected_judge', 'unknown')})"
            )
            blame = afs.get("blame_set") or []
            if blame:
                lines.append(f"Blamed objects: {', '.join(blame[:5])}")
            lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Structured-JSON context builders (Phase 7)
# ---------------------------------------------------------------------------

def _build_cluster_data(clusters: list[dict], *, top_n: int = 5) -> list[dict]:
    """Convert failure clusters into structured dicts for JSON context.

    Bug #4 (P2.2) — returns AFS projections, never raw benchmark text.
    Previously this function copied ``question``/``expected_sql`` /
    ``generated_sql`` + result samples per cluster; that was the primary
    raw-text carrier into downstream LLM prompts. Now every cluster is
    projected through ``format_afs`` which enforces the closed schema.
    """
    from genie_space_optimizer.optimization.afs import format_afs

    hard = [c for c in clusters if c.get("signal_type") != "soft"]
    sorted_hard = sorted(
        hard, key=lambda c: len(c.get("question_ids", [])), reverse=True,
    )
    return [format_afs(c) for c in sorted_hard]


def _build_soft_signal_data(soft_clusters: list[dict]) -> list[dict]:
    """Convert soft-signal clusters into structured dicts."""
    if not soft_clusters:
        return []
    _info_judges = {j for j, t in DEFAULT_THRESHOLDS.items() if t == 0.0}
    filtered: list[dict] = []
    for sc in soft_clusters:
        judges_in = {
            fj.get("judge", "")
            for qt in sc.get("question_traces", [])
            for fj in qt.get("failed_judges", [])
        }
        if judges_in and judges_in <= _info_judges:
            continue
        filtered.append(sc)
    result: list[dict] = []
    for sc in filtered[:10]:
        entry: dict[str, Any] = {
            "cluster_id": sc.get("cluster_id", "?"),
            "root_cause": sc.get("root_cause", "unknown"),
            "question_count": len(sc.get("question_ids", [])),
        }
        traces: list[dict] = []
        for qt in sc.get("question_traces", [])[:2]:
            t: dict[str, Any] = {"question": qt.get("question_text", "")[:120]}
            judges_detail = []
            for fj in qt.get("failed_judges", []):
                judges_detail.append({
                    "judge": fj.get("judge", "?"),
                    "root_cause": fj.get("resolved_root_cause", "?"),
                    "rationale": fj.get("rationale_snippet", "")[:150],
                })
            if judges_detail:
                t["failed_judges"] = judges_detail
            traces.append(t)
        if traces:
            entry["traces"] = traces
        result.append(entry)
    return result


def _parse_struct_field_names(data_type: str) -> list[str]:
    """Return top-level field names from a Spark ``struct<…>`` data type.

    Handles nested types correctly by tracking angle / paren depth so a
    ``struct<a:int, b:struct<c:int, d:int>, e:array<int>>`` returns
    ``["a", "b", "e"]`` and not ``["a", "b", "c", "d", "e"]``. Returns an
    empty list when the type is not a struct or cannot be parsed.

    The LLM uses this to distinguish struct columns (``foo.bar`` is a
    nested-field reference) from separate dim tables of the same name
    (``foo.bar`` is an ``UNRESOLVED_COLUMN`` unless joined). Mis-mapping
    one onto the other was the root cause of the ``dim_date`` hallucination.
    """
    if not data_type:
        return []
    s = data_type.strip()
    if not s.lower().startswith("struct<") or not s.endswith(">"):
        return []
    body = s[len("struct<"):-1]

    fields: list[str] = []
    depth = 0
    cursor = 0
    for i, ch in enumerate(body):
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        elif ch == "," and depth == 0:
            chunk = body[cursor:i]
            cursor = i + 1
            colon = chunk.find(":")
            if colon > 0:
                fields.append(chunk[:colon].strip())
    chunk = body[cursor:]
    colon = chunk.find(":")
    if colon > 0:
        fields.append(chunk[:colon].strip())
    return [f for f in fields if f]


def _collect_related_tables_by_identifier(
    metadata_snapshot: dict,
) -> dict[str, list[dict[str, str]]]:
    """Index ``join_specs`` by identifier so each table sees its joinable peers.

    Returns ``{lower_identifier: [{table, join_on}, …]}``. ``join_on`` is the
    rendered ``L.col = R.col`` form when extractable, otherwise the spec's
    ``description``. Surfacing this in the schema brief tells the LLM that
    e.g. a sibling ``mv_<domain>_dim_<entity>`` is reached via JOIN, not
    as a struct column on a sibling MV.
    """
    ds = metadata_snapshot.get("data_sources", {}) or {}
    if not isinstance(ds, dict):
        ds = {}
    _inst = metadata_snapshot.get("instructions", {})
    if not isinstance(_inst, dict):
        _inst = {}
    join_specs = (
        metadata_snapshot.get("join_specs", [])
        or _inst.get("join_specs", [])
        or ds.get("join_specs", [])
        or []
    )
    by_id: dict[str, list[dict[str, str]]] = {}
    seen_pairs: set[tuple[str, str, str]] = set()
    for spec in join_specs:
        if not isinstance(spec, dict):
            continue
        left_obj = spec.get("left", {})
        right_obj = spec.get("right", {})
        if isinstance(left_obj, dict) and isinstance(right_obj, dict):
            lt = str(left_obj.get("identifier", "")).strip()
            rt = str(right_obj.get("identifier", "")).strip()
            l_col = str(left_obj.get("column", left_obj.get("column_name", ""))).strip()
            r_col = str(right_obj.get("column", right_obj.get("column_name", ""))).strip()
        else:
            lt = str(spec.get("left_table_name", "")).strip()
            rt = str(spec.get("right_table_name", "")).strip()
            l_col = str(spec.get("left_column_name", "")).strip()
            r_col = str(spec.get("right_column_name", "")).strip()
        if not lt or not rt:
            continue
        if l_col and r_col:
            join_on = f"{lt.split('.')[-1]}.{l_col} = {rt.split('.')[-1]}.{r_col}"
        else:
            join_on = str(spec.get("description", "") or "").strip()
        for src, dst in ((lt, rt), (rt, lt)):
            key = (src.lower(), dst.lower(), join_on)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            by_id.setdefault(src.lower(), []).append(
                {"table": dst, "join_on": join_on} if join_on else {"table": dst}
            )
    return by_id


def _build_schema_data(
    metadata_snapshot: dict,
    filter_tables: set[str] | None = None,
) -> list[dict]:
    """Build schema context as structured list of table dicts.

    Each column entry now carries ``kind: "struct"`` plus a ``fields`` list
    when the column's ``data_type`` is a Spark struct, so the LLM can tell a
    nested field reference apart from a reference to a separately-joined dim
    table of the same name. Each table entry also carries ``related_tables``
    derived from ``join_specs`` so dim tables look like joinable peers, not
    nested fields. Together these prevent the LLM from analogising
    ``dim_location.region`` (a real struct field) onto ``dim_date.year``
    (a separate MV that must be JOINed).
    """
    from genie_space_optimizer.optimization.structured_metadata import (
        deduplicate_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", []) or []
    related_by_id = _collect_related_tables_by_identifier(metadata_snapshot)
    result: list[dict] = []
    for tbl in list(tables) + list(mvs):
        identifier = tbl.get("identifier", "")
        if filter_tables is not None:
            tbl_id = (identifier or "").lower()
            if tbl_id not in filter_tables:
                continue
        tbl_desc = tbl.get("description", [])
        if isinstance(tbl_desc, list):
            tbl_desc = "\n".join(tbl_desc)
        tbl_desc = deduplicate_structured_description(tbl_desc)
        columns: list[dict] = []
        for cc in tbl.get("column_configs", []):
            col_name = cc.get("column_name", "")
            data_type = cc.get("data_type", "")
            desc = cc.get("description", [])
            if isinstance(desc, list):
                desc = "\n".join(desc) if desc else ""
            desc = deduplicate_structured_description(desc) if desc else ""
            uc_comment = cc.get("uc_comment", "")
            if not desc and uc_comment:
                desc = uc_comment
            col_entry: dict[str, Any] = {"name": col_name}
            if data_type:
                col_entry["type"] = data_type
                struct_fields = _parse_struct_field_names(data_type)
                if struct_fields:
                    col_entry["kind"] = "struct"
                    col_entry["fields"] = struct_fields
            if desc:
                col_entry["description"] = desc
            syns = cc.get("synonyms", [])
            if syns:
                col_entry["synonyms"] = syns
            columns.append(col_entry)
        entry: dict[str, Any] = {"table": identifier}
        if tbl_desc:
            entry["description"] = tbl_desc
        if columns:
            entry["columns"] = columns
        related = related_by_id.get((identifier or "").lower(), [])
        if related:
            entry["related_tables"] = related
        result.append(entry)
    return result


def _build_structured_table_data(
    metadata_snapshot: dict,
    blame_set: list[str] | None,
) -> list[dict]:
    """Build structured table metadata as dicts with editability info."""
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", []) or []
    mv_identifiers = {
        (m.get("identifier") or "").rsplit(".", 1)[-1].lower()
        for m in mvs
    }

    blame_lower: set[str] = set()
    if blame_set:
        for b in blame_set:
            bl = b.lower().strip()
            blame_lower.add(bl)
            if "." in bl:
                blame_lower.add(bl.rsplit(".", 1)[-1])

    owned_sections = LEVER_SECTION_OWNERSHIP.get(1, set())
    result: list[dict] = []

    for tbl in tables:
        identifier = tbl.get("identifier", "")
        short_name = identifier.rsplit(".", 1)[-1].lower() if identifier else ""
        is_mv = short_name in mv_identifiers

        if blame_lower:
            tbl_match = short_name in blame_lower or identifier.lower() in blame_lower
            col_match = any(
                (cc.get("column_name") or "").lower() in blame_lower
                for cc in tbl.get("column_configs", [])
            )
            if not tbl_match and not col_match:
                continue

        etype = "mv_table" if is_mv else "table"
        template_sections = ENTITY_TYPE_TEMPLATES.get(etype, [])
        desc = tbl.get("description", [])
        desc_text = "\n".join(desc) if isinstance(desc, list) else str(desc or "")
        sections = parse_structured_description(desc_text)

        tbl_entry: dict[str, Any] = {
            "table": identifier,
            "entity_type": etype,
            "sections": {},
        }
        for sk in template_sections:
            value = sections.get(sk, "").strip()
            editable = sk in owned_sections
            tbl_entry["sections"][SECTION_LABELS[sk]] = {
                "value": value or None,
                "editable": editable,
            }
        preamble = sections.get("_preamble", "").strip()
        if preamble:
            tbl_entry["legacy_text"] = preamble
        result.append(tbl_entry)

    return result


def _build_structured_column_data(
    metadata_snapshot: dict,
    blame_set: list[str] | None,
) -> list[dict]:
    """Build structured column metadata as dicts with editability info."""
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        classify_column,
        entity_type_for_column,
        extract_synonyms_section,
        format_synonyms_section,
        merge_synonyms,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", []) or []
    mv_identifiers = {
        (m.get("identifier") or "").rsplit(".", 1)[-1].lower()
        for m in mvs
    }

    blame_lower: set[str] = set()
    if blame_set:
        for b in blame_set:
            bl = b.lower().strip()
            blame_lower.add(bl)
            if "." in bl:
                blame_lower.add(bl.rsplit(".", 1)[-1])

    owned_sections = LEVER_SECTION_OWNERSHIP.get(1, set()) | LEVER_SECTION_OWNERSHIP.get(2, set())
    result: list[dict] = []
    columns_shown = 0
    max_columns = 40

    for tbl in tables:
        identifier = tbl.get("identifier", "")
        short_name = identifier.rsplit(".", 1)[-1].lower() if identifier else ""
        is_mv = short_name in mv_identifiers

        if blame_lower:
            tbl_match = short_name in blame_lower or identifier.lower() in blame_lower
            col_match = any(
                (cc.get("column_name") or "").lower() in blame_lower
                for cc in tbl.get("column_configs", [])
            )
            if not tbl_match and not col_match:
                continue

        tbl_columns: list[dict] = []
        for cc in tbl.get("column_configs", []):
            if columns_shown >= max_columns:
                break
            col_name = cc.get("column_name", "")
            if not col_name:
                continue
            data_type = cc.get("data_type", "")
            desc = cc.get("description", [])
            syns = cc.get("synonyms", [])
            uc_comment = cc.get("uc_comment", "")
            desc_text = "\n".join(desc) if isinstance(desc, list) else str(desc or "")
            if not desc_text and uc_comment:
                desc_text = uc_comment
            sections = parse_structured_description(desc_text)
            if syns:
                existing_syn = extract_synonyms_section(sections)
                all_syns = merge_synonyms(existing_syn, syns)
                sections["synonyms"] = format_synonyms_section(all_syns)
            etype = entity_type_for_column(col_name, data_type, is_in_metric_view=is_mv)
            kind = classify_column(col_name, data_type, is_in_metric_view=is_mv)
            template_secs = ENTITY_TYPE_TEMPLATES.get(etype, [])
            col_entry: dict[str, Any] = {
                "name": col_name,
                "type": data_type or "unknown",
                "classification": kind,
                "sections": {},
            }
            for sk in template_secs:
                value = sections.get(sk, "").strip()
                editable = sk in owned_sections
                col_entry["sections"][SECTION_LABELS[sk]] = {
                    "value": value or None,
                    "editable": editable,
                }
            preamble = sections.get("_preamble", "").strip()
            if preamble:
                col_entry["legacy_text"] = preamble
            _profile = metadata_snapshot.get("_data_profile", {})
            _tbl_profile = _profile.get(identifier, {}) or _profile.get(identifier.lower(), {})
            _col_profile = _tbl_profile.get("columns", {}).get(col_name, {})
            if _col_profile.get("distinct_values"):
                col_entry["data_values"] = _col_profile["distinct_values"]
            elif _col_profile.get("min") is not None:
                col_entry["data_range"] = [_col_profile["min"], _col_profile["max"]]
            tbl_columns.append(col_entry)
            columns_shown += 1

        if tbl_columns:
            result.append({"table": identifier, "columns": tbl_columns})
        if columns_shown >= max_columns:
            break

    return result


def _build_structured_function_data(metadata_snapshot: dict) -> list[dict]:
    """Build structured function metadata as dicts."""
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])
    if not funcs:
        return []

    owned_sections = LEVER_SECTION_OWNERSHIP.get(3, set())
    template_sections = ENTITY_TYPE_TEMPLATES.get("function", [])
    result: list[dict] = []

    for fn in funcs:
        name = fn.get("name") or fn.get("identifier", "")
        comment = fn.get("comment") or fn.get("description") or ""
        if isinstance(comment, list):
            comment = "\n".join(comment)
        sections = parse_structured_description(comment)
        fn_entry: dict[str, Any] = {"name": name, "sections": {}}
        for sk in template_sections:
            value = sections.get(sk, "").strip()
            editable = sk in owned_sections
            fn_entry["sections"][SECTION_LABELS[sk]] = {
                "value": value or None,
                "editable": editable,
            }
        preamble = sections.get("_preamble", "").strip()
        if preamble:
            fn_entry["legacy_text"] = preamble
        result.append(fn_entry)
    return result


def _build_join_specs_data(metadata_snapshot: dict) -> list[dict]:
    """Build join specifications as structured dicts with clean relationship types."""
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    inst = metadata_snapshot.get("instructions", {})
    if not isinstance(inst, dict):
        inst = {}
    specs = (
        metadata_snapshot.get("join_specs", [])
        or inst.get("join_specs", [])
        or ds.get("join_specs", [])
    )
    if not specs:
        return []
    result: list[dict] = []
    for js in specs:
        left = js.get("left", {})
        right = js.get("right", {})
        sql_raw = js.get("sql", "")

        condition_text = ""
        relationship = None
        if isinstance(sql_raw, list):
            conditions = [s for s in sql_raw if not str(s).startswith("--rt=")]
            rt_parts = [s for s in sql_raw if str(s).startswith("--rt=")]
            condition_text = " AND ".join(str(c) for c in conditions)
            if rt_parts:
                rt_str = str(rt_parts[0]).strip("-").removeprefix("rt=")
                relationship = rt_str.replace("FROM_RELATIONSHIP_TYPE_", "").replace("_", " ").lower()
        else:
            condition_text = str(sql_raw)[:200]

        entry: dict[str, Any] = {
            "left_table": left.get("identifier", "?"),
            "right_table": right.get("identifier", "?"),
            "condition": condition_text[:200],
        }
        if relationship:
            entry["relationship"] = relationship
        result.append(entry)
    return result


def _build_example_sqls_data(metadata_snapshot: dict) -> list[dict]:
    """Build existing example SQL as structured dicts."""
    example_sqls = _get_existing_example_sqls(metadata_snapshot)
    if not example_sqls:
        return []
    result: list[dict] = []
    for ex in example_sqls[:20]:
        if not isinstance(ex, dict):
            continue
        q = ex.get("question", "")
        if isinstance(q, list):
            q = q[0] if q else ""
        sql = ex.get("sql", "")
        if isinstance(sql, list):
            sql = sql[0] if sql else ""
        if not q:
            continue
        entry: dict[str, Any] = {"question": q, "sql": sql[:200]}
        params = ex.get("parameters", [])
        if params:
            entry["parameters"] = [
                {"name": p.get("name", "?"), "type": p.get("type_hint", "STRING")}
                for p in params if isinstance(p, dict)
            ]
        guidance = ex.get("usage_guidance", [])
        if guidance:
            g = guidance[0] if isinstance(guidance, list) else str(guidance)
            entry["guidance"] = g[:150]
        result.append(entry)
    return result


def _build_blamed_values_data(
    clusters: list[dict],
    data_profile: dict,
    *,
    max_columns: int = 20,
) -> dict[str, dict]:
    """Build blamed column value profiles as structured dict."""
    if not data_profile:
        return {}

    blamed_refs: set[str] = set()
    for cluster in clusters:
        blame = _normalize_blame(cluster.get("asi_blame_set"))
        for b in blame:
            blamed_refs.add(b.strip().lower())

    if not blamed_refs:
        return {}

    profile_lower: dict[str, dict] = {}
    for table_fqn, tinfo in data_profile.items():
        tkey = table_fqn.lower()
        short_name = tkey.split(".")[-1] if "." in tkey else tkey
        for col_name, cinfo in tinfo.get("columns", {}).items():
            col_lower = col_name.lower()
            profile_lower[f"{tkey}.{col_lower}"] = cinfo
            profile_lower[f"{short_name}.{col_lower}"] = cinfo
            profile_lower[col_lower] = cinfo

    result: dict[str, dict] = {}
    matched = 0
    for ref in sorted(blamed_refs):
        if matched >= max_columns:
            break
        cinfo = profile_lower.get(ref)
        if not cinfo:
            continue
        matched += 1
        entry: dict[str, Any] = {}
        card = cinfo.get("cardinality")
        if card is not None:
            entry["cardinality"] = card
        vals = cinfo.get("distinct_values")
        if vals:
            entry["values"] = vals
        minv = cinfo.get("min")
        maxv = cinfo.get("max")
        if minv is not None:
            entry["range"] = [minv, maxv]
        result[ref] = entry

    return result


def _extract_tables_from_clusters(clusters: list[dict]) -> set[str] | None:
    """Extract relevant table identifiers from SQL diffs and blame sets.

    Returns a lowercased set of fully-qualified or short table names, or
    ``None`` if nothing could be extracted (which disables filtering).
    """
    tables: set[str] = set()
    for cluster in clusters:
        blame = _normalize_blame(cluster.get("asi_blame_set"))
        for b in blame:
            bl = b.lower().strip()
            if ":table" in bl:
                tables.add(bl.split(":")[0])
            else:
                tables.add(bl)
        for ctx in cluster.get("sql_contexts", [])[:3]:
            for key in ("expected_sql", "generated_sql"):
                for m in re.finditer(r'\b(\w+\.\w+\.\w+)\b', ctx.get(key, "")):
                    tables.add(m.group(1).lower())
    return tables if tables else None


_ADAPTIVE_PROMPT_TOKEN_SAFETY_MARGIN = 500


def _adaptive_context_budget_tokens() -> int:
    """Return the per-iteration context budget for the adaptive strategist.

    The adaptive prompt's rendered shell (role + RCA contract +
    instructions + identifier allowlist + output schema) consumes
    several thousand tokens before ``context_json`` is interpolated.
    Reserve room for the rendered overhead (estimated from the static
    template length plus a small safety margin) so context truncation
    cannot push the final rendered prompt over ``PROMPT_TOKEN_BUDGET``.
    """
    from genie_space_optimizer.common.config import (
        ADAPTIVE_STRATEGIST_PROMPT,
        PROMPT_TOKEN_BUDGET,
    )

    template_tokens = _estimate_tokens(ADAPTIVE_STRATEGIST_PROMPT)
    reserved = template_tokens + _ADAPTIVE_PROMPT_TOKEN_SAFETY_MARGIN
    budget = max(1_000, int(PROMPT_TOKEN_BUDGET) - reserved)
    return min(budget, int(PROMPT_TOKEN_BUDGET) - 1)


def _truncate_context_to_budget(context: dict, budget_tokens: int) -> dict:
    """Truncate context dict to fit within token budget.

    Removes entries from lowest-priority sections first:
    1. Remove soft_signal_clusters entries
    2. Remove non-blamed schema table entries
    3. Trim column lists per table in schema
    4. Trim example_sqls
    5. Trim blamed_column_values
    Never touches failure_clusters or priority_analysis.
    """
    import copy as _copy

    def _estimate(d: dict) -> int:
        return _estimate_tokens(json.dumps(d, default=str))

    current = _estimate(context)
    if current <= budget_tokens:
        return context

    result = _copy.deepcopy(context)

    if current > budget_tokens and result.get("soft_signal_clusters"):
        result["soft_signal_clusters"] = result["soft_signal_clusters"][:3]
        current = _estimate(result)

    if current > budget_tokens and result.get("soft_signal_clusters"):
        result["soft_signal_clusters"] = []
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("schema"), list):
        half = max(3, len(result["schema"]) // 2)
        result["schema"] = result["schema"][:half]
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("schema"), list):
        for tbl in result["schema"]:
            if isinstance(tbl.get("columns"), list) and len(tbl["columns"]) > 10:
                tbl["columns"] = tbl["columns"][:10]
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("existing_example_sqls"), list):
        result["existing_example_sqls"] = result["existing_example_sqls"][:5]
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("blamed_column_values"), dict):
        items = list(result["blamed_column_values"].items())
        result["blamed_column_values"] = dict(items[:10])
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("structured_metadata"), dict):
        sm = result["structured_metadata"]
        if isinstance(sm.get("functions"), list):
            sm["functions"] = sm["functions"][:3]
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("schema"), list):
        result["schema"] = result["schema"][:3]
        current = _estimate(result)

    return result


def _iq_scan_strategist_enabled() -> bool:
    """Return True when the strategist prompt should include IQ Scan findings."""
    return os.environ.get("GSO_ENABLE_IQ_SCAN_STRATEGIST", "false").lower() in {
        "1", "true", "yes", "on",
    }


def _format_strategist_budget_preamble(*, budget: int, n_clusters: int) -> str:
    """Render the ``PATCH BUDGET`` preamble prepended to the strategist prompt.

    Surfaces the per-AG patch cap and active-cluster count so the
    strategist sizes ActionGroups within the cap rather than emitting
    bundles the cap will collapse.
    """
    return (
        f"PATCH BUDGET: each ActionGroup is capped at {budget} applied patches. "
        f"Active hard clusters: {n_clusters}. "
        f"Bundle clusters only when their root causes are truly defect-compatible — "
        f"otherwise emit one ActionGroup per cluster so the cap does not collapse them."
    )


def format_prior_dropped_causal_patches_text(
    drops: list[dict] | tuple,
) -> str:
    """Cycle 5 T2 closeout — render the prior iteration's gate-drops
    of causal-target patches into a strategist-prompt block.

    Each ``DroppedCausalPatch`` is summarised on one line with its
    gate, drop reason, patch type, target table, and the
    outside-target dependents that triggered the drop. The
    strategist sees this BEFORE the cluster narrative so it can
    propose a narrower variant or shift levers instead of re-emitting
    the same dropped pattern.

    Returns an empty string when ``drops`` is empty so the caller can
    skip the prepend without a special case.
    """
    if not drops:
        return ""
    lines = [
        "PRIOR-ITERATION DROPPED CAUSAL PATCHES — these were rejected "
        "by safety gates and SHOULD NOT be re-emitted verbatim. "
        "Propose a narrower variant or shift to a different lever:",
    ]
    for d in drops:
        # ``d`` may be a frozen dataclass (DroppedCausalPatch) or a
        # plain dict if the harness already converted via to_dict.
        gate = getattr(d, "gate", None) if not isinstance(d, dict) else d.get("gate")
        reason = (
            getattr(d, "reason", None) if not isinstance(d, dict)
            else d.get("reason")
        )
        patch_type = (
            getattr(d, "patch_type", None) if not isinstance(d, dict)
            else d.get("patch_type")
        )
        target = (
            getattr(d, "target", None) if not isinstance(d, dict)
            else d.get("target")
        )
        target_qids = (
            getattr(d, "target_qids", ()) if not isinstance(d, dict)
            else d.get("target_qids", ())
        )
        dependents = (
            getattr(d, "dependents_outside_target", ())
            if not isinstance(d, dict)
            else d.get("dependents_outside_target", ())
        )
        lines.append(
            f"  - gate={gate or '?'} reason={reason or '?'} "
            f"patch_type={patch_type or '?'} target={target or '?'} "
            f"target_qids={list(target_qids)} "
            f"outside_target_dependents={list(dependents)}"
        )
    return "\n".join(lines)


def _format_iteration_feedback_block(iteration_feedback: Any) -> str:
    """Phase 3 T3.1.6 — render IterationFeedback as a strategist-prompt
    block.

    The block surfaces the prior iteration's four-tier acceptance class,
    which targets were fixed vs. still hard, the regression-debt
    classification, the AG shapes already tried per target, and any
    near-miss reflections that point the strategist at an explicit next
    action (e.g. "different repair archetype" for NET_WIN_WITH_DEBT or
    "either" for DIAGNOSTIC_HOLD).

    Returns "" when the packet is missing/empty so the caller can skip
    the prepend without a special case.
    """
    if iteration_feedback is None:
        return ""
    lines: list[str] = [
        "PRIOR ITERATION FEEDBACK — read before proposing the next "
        "ActionGroup. The prior iteration's outcome and which "
        "AG-shapes were tried are listed below; do not repeat a "
        "(repair_archetype, target_scope) shape that already failed:",
    ]
    try:
        acc_class = str(
            getattr(iteration_feedback, "acceptance_class", "") or ""
        )
        delta_pp = float(
            getattr(iteration_feedback, "delta_pp", 0.0) or 0.0
        )
        accept = bool(getattr(iteration_feedback, "accept", False))
        lines.append(
            f"  - acceptance_class={acc_class} accept={accept} "
            f"delta_pp={delta_pp:+.2f}"
        )
        targets = tuple(
            getattr(iteration_feedback, "target_qids", ()) or ()
        )
        fixed = tuple(
            getattr(iteration_feedback, "target_fixed_qids", ()) or ()
        )
        still_hard = tuple(
            getattr(iteration_feedback, "target_still_hard_qids", ()) or ()
        )
        lines.append(
            f"  - targets={list(targets)} fixed={list(fixed)} "
            f"still_hard={list(still_hard)}"
        )
        debt = str(
            getattr(iteration_feedback, "regression_debt_classification", "")
            or ""
        )
        if debt:
            lines.append(f"  - regression_debt={debt}")
        tried = (
            getattr(iteration_feedback, "tried_ag_shapes_by_target", {}) or {}
        )
        if tried:
            lines.append("  - tried AG shapes (do not repeat):")
            for qid in sorted(tried.keys()):
                shapes = tried.get(qid) or ()
                shape_strs = [
                    f"({s.repair_archetype}, {s.target_scope})"
                    if hasattr(s, "repair_archetype")
                    else str(s)
                    for s in shapes
                ]
                lines.append(f"      {qid}: {shape_strs}")
        reflections = (
            getattr(iteration_feedback, "near_miss_reflections", ()) or ()
        )
        if reflections:
            lines.append("  - near-miss reflections from prior iteration:")
            for r in reflections:
                kind = getattr(r, "kind", "")
                req = getattr(r, "required_next_iter_change", "")
                arch = getattr(r, "prior_repair_archetype", "")
                scope = getattr(r, "prior_target_scope", "")
                lines.append(
                    f"      kind={kind} required_change={req} "
                    f"prior=({arch}, {scope})"
                )
    except Exception:
        return ""
    return "\n".join(lines)


def format_strategist_ranking_text(
    priority_ranking: list[dict],
    *,
    top_n: int = 10,
) -> str:
    """Render the strategist's per-cluster priority ranking text.

    Cycle 2 Task 4 closeout — when ``cluster["recommended_levers"]`` is
    set (post-``rank_clusters`` stamp via
    ``stages.action_groups.stamp_recommended_levers_on_clusters``),
    the per-cluster lever hint is appended to the cluster's ranking
    line so the strategist LLM sees ``recommended_levers=[...]``
    alongside cluster identity. Clusters without the field render
    as before (backwards-compatible — pre-extraction format
    preserved verbatim).
    """
    ranking_lines: list[str] = []
    for c in priority_ranking[:top_n]:
        line = (
            f"  Rank {c.get('rank', '?')}: "
            f"[{c.get('cluster_id', '?')}] {c.get('root_cause', '?')} "
            f"(judge={c.get('affected_judge', '?')}, "
            f"questions={len(c.get('question_ids', []))}, "
            f"impact={c.get('impact_score', 0):.1f})"
        )
        levers = c.get("recommended_levers")
        if levers:
            line += f" recommended_levers={list(levers)}"
        ranking_lines.append(line)
    return "\n".join(ranking_lines) if ranking_lines else "(no clusters)"


def _format_iq_scan_findings(scan_summary: dict | None) -> str:
    """Render the scan summary for the strategist prompt.

    Returns an empty string when the strategist flag is disabled or the
    summary is absent. Each section is omitted when the corresponding field
    is empty so the prompt stays compact.
    """
    if not _iq_scan_strategist_enabled() or not scan_summary:
        return ""

    lines: list[str] = []
    score = scan_summary.get("score")
    total = scan_summary.get("total", 12)
    maturity = scan_summary.get("maturity")
    if score is not None and maturity:
        lines.append(f"IQ Score: {score}/{total} ({maturity})")

    ceilings = scan_summary.get("ceilings") or []
    for ceiling in ceilings:
        lines.append(f"WARNING: {ceiling}")

    rls = scan_summary.get("rls_tables") or []
    if rls:
        lines.append(
            "Tables with row-level security (entity matching is silently disabled here): "
            + ", ".join(rls)
        )

    gaps = scan_summary.get("coverage_gaps") or []
    if gaps:
        lines.append("Coverage gaps: " + "; ".join(gaps))

    levers = scan_summary.get("recommended_levers") or []
    if levers:
        # Import locally to avoid cycles and to pick up LEVER_NAMES at call time.
        from genie_space_optimizer.common.config import LEVER_NAMES
        pretty = [f"{lv} ({LEVER_NAMES.get(lv, '?')})" for lv in levers]
        lines.append("Scan-recommended levers: " + ", ".join(pretty))

    return "\n".join(lines)


def _field(obj: Any, name: str, default: Any = "") -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _theme_target_qids(theme: Any) -> tuple[str, ...]:
    values = getattr(theme, "target_qids", None)
    if isinstance(theme, dict):
        values = theme.get("target_qids")
    return tuple(str(q).strip() for q in (values or ()) if str(q).strip())


def _theme_matches_target_qids(
    theme: Any,
    target_qids: Iterable[str] | None = None,
) -> bool:
    """Return True if the theme should be eligible for the current AG.

    When ``target_qids`` is empty/None the legacy global behavior applies
    (every theme is eligible) so existing call sites without a known AG
    target set keep working.
    """
    requested = {str(q).strip() for q in (target_qids or ()) if str(q).strip()}
    if not requested:
        return True
    return bool(requested.intersection(_theme_target_qids(theme)))


def _target_qids_for_rca_bridges(
    action_group: dict,
    strategy: dict,
    metadata_snapshot: dict,
) -> tuple[str, ...]:
    """Resolve the canonical AG target QIDs used to scope RCA bridge themes."""
    source_clusters = (
        strategy.get("_source_clusters")
        or metadata_snapshot.get("_failure_clusters")
        or metadata_snapshot.get("failure_clusters")
        or []
    )
    try:
        from genie_space_optimizer.optimization.control_plane import (
            target_qids_from_action_group,
        )

        return target_qids_from_action_group(action_group, source_clusters)
    except Exception:
        return tuple(
            str(q).strip()
            for q in (action_group.get("affected_questions") or ())
            if str(q).strip()
        )


def _rca_themes_requesting_synthesis(
    themes: list[Any],
    target_qids: Iterable[str] | None = None,
) -> list[Any]:
    out: list[Any] = []
    for theme in themes or []:
        if not _theme_matches_target_qids(theme, target_qids):
            continue
        patches = getattr(theme, "patches", None)
        if isinstance(theme, dict):
            patches = theme.get("patches")
        for patch in patches or ():
            if isinstance(patch, dict) and patch.get("type") == "request_example_sql_synthesis":
                out.append(theme)
                break
    return out


def _append_teaching_kit_support_proposals(
    proposals: list[dict],
    *,
    synth_proposal: dict,
    provenance_base: dict,
    ag_id: str,
    source_cluster_id: str = "",
) -> None:
    """Append teaching-kit support patches to ``proposals`` in place.

    Shared between the strategist-driven Lever 5 cluster synthesis branch
    and the RCA-theme-driven cluster synthesis branch so kit shape stamping
    (``kit_id``, ``target_qids``, provenance, dual_persistence, defaults)
    is identical regardless of which path produced the primary example.
    """
    for support in synth_proposal.get("_supporting_proposals", []) or []:
        if not isinstance(support, dict):
            continue
        support.setdefault("proposal_id", f"P{len(proposals) + 1:03d}")
        support.setdefault("cluster_id", f"{ag_id}_KIT")
        support.setdefault("lever", support.get("lever", 5))
        support.setdefault("scope", "genie_config")
        support.setdefault(
            "change_description",
            f"[{ag_id}] Teaching kit support: {support.get('patch_type', '?')}",
        )
        support.setdefault(
            "rationale",
            f"Support patch for teaching kit {synth_proposal.get('kit_id', '')}",
        )
        support.setdefault(
            "dual_persistence",
            DUAL_PERSIST_PATHS.get(int(support.get("lever", 5)), DUAL_PERSIST_PATHS[5]),
        )
        support.setdefault("confidence", 0.8)
        support.setdefault(
            "questions_fixed",
            len(synth_proposal.get("target_qids", []) or []),
        )
        support.setdefault("questions_at_risk", 0)
        support.setdefault("net_impact", 0.7)
        support.setdefault("target_qids", synth_proposal.get("target_qids", []))
        support.setdefault("kit_id", synth_proposal.get("kit_id", ""))
        support["provenance"] = {
            **provenance_base,
            **(support.get("provenance") or {}),
            "patch_type": support.get("patch_type", ""),
            "synthesis_source": "cluster_driven_teaching_kit",
            "kit_id": support.get("kit_id", ""),
            "source_cluster_id": source_cluster_id,
        }
        proposals.append(support)


def _cluster_from_rca_example_theme(theme: Any) -> dict:
    """Project an RCA theme into a cluster-shaped dict for cluster-driven synthesis.

    The cluster-driven engine reads ``cluster_id``, ``root_cause``,
    ``asi_blame_set``, ``asi_counterfactual_fixes``, and ``question_ids``
    via ``format_afs``. We synthesize that shape from the RCA theme so the
    same engine can be reused for both strategist-driven and RCA-driven
    Example SQL requests.
    """
    patches = list(getattr(theme, "patches", ()) or ())
    synth_patch = next(
        (
            p for p in patches
            if isinstance(p, dict)
            and p.get("type") == "request_example_sql_synthesis"
        ),
        {},
    )
    target_qids = [
        str(q)
        for q in (getattr(theme, "target_qids", ()) or ())
        if str(q)
    ]
    blame_set = synth_patch.get("blame_set") or list(
        getattr(theme, "touched_objects", ()) or ()
    )
    return {
        "cluster_id": str(getattr(theme, "rca_id", "") or "rca_theme"),
        "root_cause": str(
            synth_patch.get("root_cause")
            or getattr(getattr(theme, "rca_kind", None), "value", "")
            or "rca_example_sql"
        ),
        "affected_judge": "rca",
        "asi_blame_set": list(blame_set or []),
        "asi_counterfactual_fixes": [
            str(
                synth_patch.get("intent")
                or "Synthesize a counterfactual teaching example for this RCA shape."
            )
        ],
        "question_ids": target_qids,
        "target_qids": target_qids,
        "rca_id": str(getattr(theme, "rca_id", "")),
        "patch_family": str(getattr(theme, "patch_family", "")),
    }


def _dispatch_plan11_synthesis_for_legacy_cluster(
    cluster: dict,
    metadata_snapshot: dict,
    *,
    benchmarks: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    w: Any = None,
    spark: Any = None,
) -> Any:
    """Plan 11 — drop-in replacement for
    :func:`run_cluster_driven_synthesis_for_single_cluster` when the
    ``GSO_PLAN11_LLM_FIRST`` flag is on.

    Converts the legacy cluster-dict shape (``cluster_id``,
    ``question_ids``, ``asi_blame_set``, ``root_cause``, etc.) into a
    :class:`FailureCluster` carrier and routes to
    :func:`run_plan11_synthesis_for_single_cluster`. The return shape is
    :class:`ClusterSynthesisResult` — same envelope the legacy
    synthesizer returns — so the wider optimizer.py flow is unchanged.

    ``benchmarks`` / ``catalog`` / ``gold_schema`` / ``warehouse_id`` /
    ``spark`` are accepted for signature parity with the legacy
    synthesizer; Plan 11 does not need them at this stage (validation
    happens later via :func:`validate_patch` with its own kwargs).
    """
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )

    failure_cluster = FailureCluster(
        cluster_id=str(cluster.get("cluster_id", "") or "C_unknown"),
        semantic_theme=str(
            cluster.get("semantic_theme")
            or cluster.get("root_cause")
            or "legacy_cluster"
        ),
        member_qids=tuple(
            str(q) for q in (
                cluster.get("member_qids")
                or cluster.get("question_ids")
                or ()
            )
        ),
        unifying_evidence=str(
            cluster.get("unifying_evidence")
            or cluster.get("llm_rationale")
            or cluster.get("root_cause", "")
        ),
        repair_hypothesis=str(
            cluster.get("repair_hypothesis")
            or cluster.get("suggested_repair_shape")
            or ""
        ),
        primary_blame_set=tuple(
            str(b) for b in (cluster.get("asi_blame_set") or ())
        ),
        confidence=cluster.get("llm_confidence", "medium"),  # type: ignore[arg-type]
    )

    iteration = int(metadata_snapshot.get("iteration") or 0)
    optimization_run_id = str(
        metadata_snapshot.get("optimization_run_id")
        or metadata_snapshot.get("run_id")
        or ""
    )
    ag_id = str(metadata_snapshot.get("ag_id") or cluster.get("ag_id") or "")

    return run_plan11_synthesis_for_single_cluster(
        cluster=failure_cluster,
        schema_slice=dict(metadata_snapshot),
        history=[],
        optimization_run_id=optimization_run_id,
        iteration=iteration,
        ag_id=ag_id,
        w=w,
    )


def _apply_evidence_to_lever_policy(
    target_lever: int,
    cluster_or_action_group: dict,
) -> int:
    """Plan 12 PR 6 — re-route Lever 1 to a generating lane when the
    cluster's evidence demands generation.

    Closes the routing bug observed in postmortem dc89d1a9-... where
    gs_004 (wrong-aggregation evidence) was routed to Lever 1
    (non-generating ``add_column_description``) despite the failure
    mode requiring a structural example-SQL or metric-view edit.

    Lookup order for the evidence_kind: ``asi_failure_type`` →
    ``root_cause`` → empty string. The policy (see
    :mod:`evidence_to_lever_policy`) decides whether Lever 1 is
    eligible. If not, the preferred family from
    :func:`eligible_lever_families` is mapped to its integer lever:

      * ``"5b"`` / ``"5"`` / ``5`` → 5
      * ``"6"``  / ``6``         → 6
      * ``"2"``  / ``2``         → 2

    Non-Lever-1 ``target_lever`` values pass through unchanged — those
    are already generating lanes and don't need the safety net.
    """
    if target_lever != 1:
        return target_lever
    from genie_space_optimizer.optimization.evidence_to_lever_policy import (
        eligible_lever_families,
        refuses_non_generating_lane,
    )
    evidence_kind = str(
        cluster_or_action_group.get("asi_failure_type")
        or cluster_or_action_group.get("root_cause")
        or ""
    )
    if not refuses_non_generating_lane(evidence_kind):
        return target_lever
    eligible = eligible_lever_families(evidence_kind)
    if not eligible:
        return target_lever  # defensive — policy never returns empty
    preferred = eligible[0]
    if preferred in ("5b", "5", 5):
        return 5
    if preferred in ("6", 6):
        return 6
    if preferred in ("2", 2):
        return 2
    return target_lever


def _build_blast_radius_drop_record(
    *,
    patch: dict,
    collateral_qids: tuple[str, ...],
    protected_sql_by_qid: dict[str, str],
) -> Any:
    """Plan 12 — construct a :class:`BlastRadiusDropRecord` from the
    dict-shaped patch + the collateral the blast-radius gate detected.

    Falls back from ``original_patch_type`` to ``patch_type`` so Plan
    11 / pre-Plan-12 patches still carry the required field. I23's
    coverage check only fires when ``original_patch_type`` is
    non-empty, so empty pre-Plan-12 records stay green.
    """
    from genie_space_optimizer.optimization.blast_radius_drop_record import (
        BlastRadiusDropRecord,
    )
    return BlastRadiusDropRecord(
        intent_id=str(patch.get("intent_id") or ""),
        original_patch_type=str(
            patch.get("original_patch_type")
            or patch.get("patch_type")
            or ""
        ),
        original_patch_body=dict(patch.get("patch_body") or {}),
        causal_target=str(patch.get("causal_target") or ""),
        failing_sql_anchor=str(patch.get("failing_sql_anchor") or ""),
        target_qids=tuple(str(q) for q in (patch.get("target_qids") or [])),
        collateral_qids=tuple(str(q) for q in collateral_qids),
        protected_sql_by_qid=dict(protected_sql_by_qid or {}),
        rca_card_id=str(patch.get("rca_card_id") or ""),
        cluster_id=str(patch.get("cluster_id") or ""),
        ag_id=str(patch.get("ag_id") or ""),
    )


def _build_plan11_failing_qids_from_typed_evidence(
    rca_evidence_typed: dict,
    eval_rows: list[dict] | None = None,
    *,
    schema_columns: tuple[str, ...] | list[str] | None = None,
) -> list[dict]:
    """Plan 11 Stage 1 input adapter.

    Projects :class:`PerQidRcaEvidence` (Plan 3's per-QID typed RCA
    output) into the ``failing_qids`` shape Plan 11's
    :func:`diagnose_failing_qids` expects.

    Trial 12 fix: when ``eval_rows`` is supplied, the matching row is
    located via the canonical QID extractor and the Stage 1 card is
    hydrated through :func:`eval_row_access.build_stage1_evidence_card`.
    Typed evidence fields win per-field (the prior contract); the row
    supplies ``question_text`` / ``ground_truth_sql`` / ``generated_sql``
    which ``PerQidRcaEvidence`` does not carry. If ``eval_rows`` is
    omitted or no row matches, the card falls back to typed-only
    hydration (legacy behavior).
    """
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )
    from genie_space_optimizer.optimization.eval_row_access import (
        build_stage1_evidence_card,
    )

    by_qid: dict[str, dict] = {}
    for row in (eval_rows or []):
        qid_str, qid_source = extract_question_id(row)
        if not qid_str or qid_source == "":
            continue
        by_qid[qid_str] = row

    out: list[dict] = []
    for qid, ev in rca_evidence_typed.items():
        row = by_qid.get(str(qid))
        out.append(
            build_stage1_evidence_card(
                str(qid),
                row,
                typed_evidence=ev,
                schema_columns=tuple(schema_columns or ()),
            )
        )
    return out


def _extract_eval_rows_for_stamping(eval_results: dict) -> list[dict]:
    """Plan 12 Step 1.3.5 — best-effort row extraction matching the
    shape-detection logic inside :func:`cluster_failures` (the legacy
    heuristic clusterer accepts an MLflow ``EvaluationResult``, a
    nested dict with ``eval_result.tables['eval_results']``, or a
    plain ``rows`` / ``eval_results`` / ``table`` list).

    Returns a list of plain dicts; callers should NEVER assume a
    pandas-shaped object survives this function. Empty list on any
    extraction failure — the stamper treats that as "no failing
    rows to derive from."
    """
    if not isinstance(eval_results, dict):
        return []
    results_obj = eval_results.get("eval_result")
    table: Any = None
    if results_obj is not None and hasattr(results_obj, "tables"):
        try:
            table = results_obj.tables.get("eval_results")
        except Exception:
            table = None
    elif results_obj is not None and hasattr(results_obj, "eval_results"):
        table = results_obj.eval_results
    if table is None:
        table = (
            eval_results.get("eval_results")
            or eval_results.get("rows")
            or eval_results.get("table")
        )
    if table is None:
        return []
    # Coerce pandas DataFrame → list of row dicts.
    if hasattr(table, "iterrows"):
        try:
            return [row.to_dict() for _, row in table.iterrows()]
        except Exception:
            return []
    if isinstance(table, list):
        return [r for r in table if isinstance(r, dict)]
    return []


def _row_is_failing(row: dict) -> bool:
    """Plan 11 dispatch — canonical hard-failure predicate.

    Delegates to :func:`evaluation.row_is_hard_failure`, the same Tier
    1.4 predicate the accuracy gate and legacy clustering use. Sharing
    this predicate closes the 2026-05-22 postmortem regression where
    the dispatch adapter saw zero failing QIDs because its score-only
    predicate didn't match the row shape carried by production
    eval_results (which has ``result_correctness`` + ``arbiter`` but
    no ``score`` field).

    A row is hard iff ``result_correctness == "no"`` AND the arbiter
    verdict is NOT in the correct set (``both_correct`` /
    ``genie_correct``). Arbiter overrides keep rc=no rows soft so they
    do not enter the optimizer's repair lane.
    """
    from genie_space_optimizer.optimization.evaluation import (
        row_is_hard_failure as _row_is_hard_failure_canonical,
    )
    return _row_is_hard_failure_canonical(row)


def _stamp_failing_qids_from_eval_results(
    eval_results: dict,
    metadata_snapshot: dict,
) -> None:
    """Plan 12 Step 1.3.5 — derive ``_failing_qids`` /
    ``_eval_rows_failing`` from ``eval_results.rows`` and stamp them on
    ``metadata_snapshot`` so :func:`_decide_and_run_plan11_dispatch`
    can build a non-empty Stage 1 input even when Plan 3's typed
    evidence is empty (the typical production case from the
    2026-05-20 postmortems).

    Idempotent / non-destructive: only stamps when ``_failing_qids`` is
    absent. Callers that explicitly pre-stamp (e.g. a future
    per-iteration harness wire-in) take precedence.
    """
    if metadata_snapshot.get("_failing_qids") is not None:
        return
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )

    rows = _extract_eval_rows_for_stamping(eval_results)
    failing = [row for row in rows if _row_is_failing(row)]
    failing_qids: list[str] = []
    for row in failing:
        # 2026-05-23 admission fix: production MLflow rows carry the qid under
        # inputs/question_id, nested inputs.question_id, or
        # request.kwargs.question_id. The previous single-key row.get
        # ("question_id") returned "" for every such row and starved Plan 11
        # dispatch with failing_qids_count=0. Delegate to the canonical
        # extractor (built in Cycle 8 for exactly this divergence). The
        # ``source`` tag is logged when it falls back to a trace-id alias
        # so producer-side qid misrouting remains visible.
        qid, qid_source = extract_question_id(row)
        if not qid:
            continue
        if qid_source == "trace_fallback":
            logger.warning(
                "Admitting hard row via trace-id fallback (client_request_id=%s); "
                "expected canonical inputs/question_id from producer.",
                qid,
            )
        failing_qids.append(qid)
    metadata_snapshot["_failing_qids"] = failing_qids
    metadata_snapshot["_eval_rows_failing"] = failing


def _build_plan11_failing_qids_from_raw(
    *,
    failing_qids: list[str],
    eval_rows: list[dict],
    rca_evidence_typed: dict | None = None,
    schema_columns: tuple[str, ...] | list[str] | None = None,
) -> list[dict]:
    """Plan 12 — fallback Plan 11 Stage 1 input adapter.

    Used when ``rca_evidence_typed`` is empty (the silent-fallthrough
    bug observed in both 2026-05-20 postmortems). Reads
    ``failing_qids + eval_rows`` directly so Plan 11's LLM lane runs
    even without Plan 3's deterministic RCA classifier.

    ``rca_evidence`` is an empty bundle (so the LLM cannot rely on
    Plan 3's free-text fields); the LLM is responsible for forming
    its own diagnosis from ``judge_rationale + generated_sql +
    ground_truth_sql``.

    Trial 13 symmetry: the optional ``rca_evidence_typed`` kwarg lets
    callers backfill typed evidence per QID when a partial map is
    available. The current dispatch branch at
    ``_decide_and_run_plan11_dispatch`` only invokes this builder when
    the typed map is empty, but threading the kwarg matches the SM
    canonical lane (``diagnose_llm._build_failing_qid_payload``) and
    the Plan 11 typed-evidence builder so all three Stage 1 input
    paths accept identical inputs.
    """
    # 2026-05-23 admission fix: production MLflow rows carry the qid
    # under inputs/question_id, nested inputs.question_id, or
    # request.kwargs.question_id. Single-key row.get("question_id")
    # excluded every such row from the by_qid dict, so the Plan 11 raw
    # fallback adapter returned an empty list even when failing_qids
    # was non-empty. Delegate to the canonical extractor.
    #
    # Trial 12 hydration fix: per-row card construction goes through
    # eval_row_access.build_stage1_evidence_card so the Stage 1 LLM
    # receives non-empty question/SQL/ASI evidence regardless of row
    # shape (top-level / slash-flattened / dotted / nested / request
    # kwargs). Trial 11 root cause was empty cards from flat row.get(...)
    # against namespaced production rows.
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )
    from genie_space_optimizer.optimization.eval_row_access import (
        build_stage1_evidence_card,
    )

    typed_by_qid: dict = rca_evidence_typed or {}
    by_qid: dict[str, dict] = {}
    for row in (eval_rows or []):
        qid_str, qid_source = extract_question_id(row)
        if not qid_str or qid_source == "":
            continue
        by_qid[qid_str] = row
    out: list[dict] = []
    for qid in failing_qids or []:
        row = by_qid.get(str(qid))
        if row is None:
            continue
        typed_ev = typed_by_qid.get(str(qid))
        out.append(
            build_stage1_evidence_card(
                str(qid),
                row,
                typed_evidence=typed_ev,
                schema_columns=tuple(schema_columns or ()),
            )
        )
    return out


def _decide_and_run_plan11_dispatch(
    *,
    failing_qids: list[str],
    rca_evidence_typed: dict,
    metadata_snapshot: dict,
    namespace: str,
    signal_type: str,
    run_id: str,
    w: Any,
) -> list[dict] | None:
    """Plan 12 — explicit Plan 11 dispatch decision with typed marker.

    Replaces the silent ``and rca_evidence_typed`` short-circuit at the
    old optimizer.py:2113. Returns ``None`` when Plan 11 was skipped
    (caller falls through to Plan 4); returns a non-empty list of
    legacy cluster dicts when Plan 11 produced clusters.

    Emits exactly one ``GSO_PLAN11_DISPATCH_DECISION_V1`` marker per
    invocation, carrying outcome ∈ {"entered","skipped"} and a typed
    ``skip_reason`` from ``VALID_PLAN11_SKIP_REASONS`` when skipped.
    """
    from genie_space_optimizer.common.config import plan11_llm_first_enabled
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_dispatch_decision_marker,
    )

    iteration_p11 = int(metadata_snapshot.get("iteration") or 0)
    optimization_run_id_p11 = str(
        metadata_snapshot.get("optimization_run_id") or run_id or ""
    )
    namespace_p11 = str(namespace or signal_type)

    def _emit_skipped(reason: str) -> None:
        print(
            plan11_dispatch_decision_marker(
                optimization_run_id=optimization_run_id_p11,
                iteration=iteration_p11,
                namespace=namespace_p11,
                outcome="skipped",
                skip_reason=reason,
                failing_qids_count=len(failing_qids or []),
                rca_evidence_typed_present=bool(rca_evidence_typed),
            )
        )

    if not plan11_llm_first_enabled():
        _emit_skipped("flag_disabled")
        return None

    if not failing_qids:
        _emit_skipped("no_failing_qids")
        return None

    # Build the Plan 11 Stage 1 input. Prefer typed evidence when
    # present (the legacy path); otherwise reconstruct from raw eval
    # rows (Plan 12 fallback — closes the silent-fallthrough bug).
    eval_rows = list(metadata_snapshot.get("_eval_rows_failing") or [])

    # Trial 13i — derive ``schema_columns`` BEFORE building the Stage 1
    # cards so the seed FQN normalizer inside
    # ``build_stage1_evidence_card`` has a non-empty universe to match
    # against on this lane (the SM lane derives the same value on
    # ``TransformerContext`` for the same reason). Stashed on
    # ``_schema_columns_batch`` for the downstream ``diagnose_failing_qids``
    # call so the LLM prompt receives the same list the card normalizer
    # used.
    from genie_space_optimizer.optimization.schema_columns import (
        _derive_schema_columns,
    )
    _schema_columns_batch, _schema_columns_source_batch = (
        _derive_schema_columns(
            metadata_snapshot=metadata_snapshot,
            rca_evidence_typed=rca_evidence_typed or None,
            uc_columns=(metadata_snapshot or {}).get("uc_columns"),
        )
    )

    if rca_evidence_typed:
        failing_qids_input = _build_plan11_failing_qids_from_typed_evidence(
            rca_evidence_typed,
            eval_rows=eval_rows,
            schema_columns=_schema_columns_batch,
        )
    else:
        failing_qids_input = _build_plan11_failing_qids_from_raw(
            failing_qids=list(failing_qids),
            eval_rows=eval_rows,
            rca_evidence_typed=rca_evidence_typed,
            schema_columns=_schema_columns_batch,
        )

    # Trial 13 Track 6 — emit the namespace-drift observability marker
    # whenever the SM-vs-Plan11 dispatch QID sets disagree. Replaces the
    # Trial 12 count-only "starved" check, which missed the
    # ``airline_..._gs_009`` vs ``gs_009`` namespace mismatch observed
    # in the 98ec8950 / dc89d1a9 postmortems. Fires BEFORE the dispatch
    # decision marker so emission ordering matches the legacy path.
    sm_hard_qid_count = int(
        metadata_snapshot.get("_sm_hard_qid_count") or 0
    )
    harness_hard_qid_count = int(
        metadata_snapshot.get("_harness_hard_qid_count") or 0
    )
    sm_hard_qids_list: list[str] = [
        str(q) for q in (metadata_snapshot.get("_sm_hard_qids") or []) if q
    ]
    plan11_dispatch_qids_list: list[str] = [
        str(card.get("qid") or "")
        for card in (failing_qids_input or [])
        if str(card.get("qid") or "").strip()
    ]
    # Provide list-based input when the SM list was threaded through
    # the metadata snapshot; otherwise the marker falls back to the
    # legacy count-based "starved" classifier so older call sites
    # remain observable.
    if sm_hard_qids_list or sm_hard_qid_count > 0:
        from genie_space_optimizer.optimization.state_machine.markers import (
            plan11_dispatch_starved_marker,
        )

        _drift_line = plan11_dispatch_starved_marker(
            run_id=optimization_run_id_p11,
            iteration=iteration_p11,
            plan11_failing_qids_count=len(plan11_dispatch_qids_list),
            sm_hard_qid_count=sm_hard_qid_count or len(sm_hard_qids_list),
            harness_hard_qid_count=harness_hard_qid_count,
            sm_hard_qids=sm_hard_qids_list or None,
            plan11_dispatch_qids=(
                plan11_dispatch_qids_list if sm_hard_qids_list else None
            ),
        )
        if _drift_line:
            print(_drift_line, flush=True)

    if not failing_qids_input:
        _emit_skipped("build_failing_qids_empty")
        return None

    # We have inputs — emit the "entered" decision before running.
    print(
        plan11_dispatch_decision_marker(
            optimization_run_id=optimization_run_id_p11,
            iteration=iteration_p11,
            namespace=namespace_p11,
            outcome="entered",
            skip_reason="",
            failing_qids_count=len(failing_qids_input),
            rca_evidence_typed_present=bool(rca_evidence_typed),
        )
    )

    # Trial 13i — emit per-QID input-quality markers on the batch lane
    # so postmortems see the same provenance + seed-normalization stats
    # whether Stage 1 ran via the SM canonical lane or the legacy batch
    # lane. Mirrors ``diagnose_llm._invoke_stage1_llm``.
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage1_input_quality_marker,
    )
    for _card in failing_qids_input:
        _seed_stats = _card.get("_seed_normalization") or {}
        # Trial 14 — derive the typed blame-kind histogram from the
        # ``_blame_structured`` stamp on the card (mirrors the SM lane
        # in ``diagnose_llm._invoke_stage1_llm``).
        _blame_structured_entries = _card.get("_blame_structured") or ()
        _blame_kind_distribution: dict[str, int] = {}
        for _entry in _blame_structured_entries:
            if not isinstance(_entry, dict):
                continue
            _kind = str(_entry.get("kind") or "").strip().lower()
            if not _kind:
                continue
            _blame_kind_distribution[_kind] = (
                _blame_kind_distribution.get(_kind, 0) + 1
            )
        print(
            plan11_stage1_input_quality_marker(
                optimization_run_id=optimization_run_id_p11,
                iteration=iteration_p11,
                qid=str(_card.get("qid") or ""),
                schema_columns_source=str(_schema_columns_source_batch),
                schema_columns_size=len(_schema_columns_batch),
                seeds_pre_normalize=int(
                    _seed_stats.get("seeds_pre_normalize") or 0
                ),
                seeds_post_normalize=int(
                    _seed_stats.get("seeds_post_normalize") or 0
                ),
                seeds_normalized=int(_seed_stats.get("seeds_normalized") or 0),
                seeds_dropped=int(_seed_stats.get("seeds_dropped") or 0),
                contract_violation=(
                    "missing_schema_columns"
                    if not _schema_columns_batch
                    else ""
                ),
                blame_kind_distribution=_blame_kind_distribution,
            ),
            flush=True,
        )

    from genie_space_optimizer.optimization.stages.cluster_plan11 import (
        cluster_diagnoses,
    )
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )

    # Trial 13i — reuse the ``schema_columns`` derived above before card
    # construction so the seed normalizer and the LLM prompt see one
    # identical list (the SM lane wires the same value onto
    # ``TransformerContext`` for the same reason).
    schema_columns_list: list[str] = list(_schema_columns_batch)

    diagnoses = diagnose_failing_qids(
        failing_qids=failing_qids_input,
        schema_columns=schema_columns_list,
        optimization_run_id=optimization_run_id_p11,
        iteration=iteration_p11,
        w=w,
    )
    if not diagnoses:
        # Stage 1 emits its own STAGE1 markers with the abstain reason.
        # Caller falls back to Plan 4 / heuristic.
        return None

    # Trial 13 Track 3 — non-actionable diagnosis hard gate (Plan 11
    # batch lane). The SM Stage 1 transformer enforces the same gate
    # for the canonical lane; this filter mirrors it so the batch
    # lane cannot smuggle a non-actionable diagnosis into Stage 2.
    # dc89d1a9 advanced 21/24 ``diagnosed`` outcomes that failed every
    # actionability check into Stage 2; Stage 3 emitted six
    # ``empty_synthesis`` and zero patches were applied.
    from genie_space_optimizer.optimization.run_analysis_contract import (
        classify_non_actionable_reason,
        plan11_stage1_non_actionable_reject_marker,
    )

    actionable_diagnoses = []
    for diag in diagnoses:
        reason = classify_non_actionable_reason(
            rca_kind_label=diag.rca_kind_label,
            evidence_summary=diag.evidence_summary,
            blame_set=diag.blame_set,
        )
        if not reason:
            actionable_diagnoses.append(diag)
            continue
        print(
            plan11_stage1_non_actionable_reject_marker(
                optimization_run_id=optimization_run_id_p11,
                iteration=iteration_p11,
                qid=str(diag.qid),
                reason=reason,
                rca_kind_label=str(diag.rca_kind_label),
                blame_set_size=len(diag.blame_set or ()),
                evidence_summary_chars=len(diag.evidence_summary or ""),
            ),
            flush=True,
        )
    if not actionable_diagnoses:
        # Every diagnosis failed the actionability gate; downstream
        # clustering / synthesis would produce empty proposals. Bail
        # out early so the caller can fall back to Plan 4 / heuristic.
        return None
    diagnoses = actionable_diagnoses

    # Pre-flight assertion: no non-actionable diagnosis may reach
    # Stage 2. If this assertion fires, a refactor reintroduced the
    # silent advance and the gate above is no longer protecting the
    # cluster input — fail loud rather than emitting empty_synthesis.
    for diag in diagnoses:
        gate_reason = classify_non_actionable_reason(
            rca_kind_label=diag.rca_kind_label,
            evidence_summary=diag.evidence_summary,
            blame_set=diag.blame_set,
        )
        assert not gate_reason, (
            f"Plan 11 Stage 2 pre-flight violated: non-actionable "
            f"diagnosis qid={diag.qid!r} reached cluster input with "
            f"reason={gate_reason!r}. The actionability gate must "
            f"filter non-actionable diagnoses before Stage 2."
        )

    failure_clusters = cluster_diagnoses(
        diagnoses=diagnoses,
        schema_columns=schema_columns_list,
        optimization_run_id=optimization_run_id_p11,
        iteration=iteration_p11,
        namespace=namespace_p11,
        w=w,
    )
    if not failure_clusters:
        return None

    return [
        _plan11_failure_cluster_to_legacy_dict(fc, signal_type=signal_type)
        for fc in failure_clusters
    ]


def _plan11_failure_cluster_to_legacy_dict(
    fc: Any,
    *,
    signal_type: str = "hard",
) -> dict:
    """Plan 11 Stage 2 output adapter.

    Projects :class:`FailureCluster` (Plan 11's typed cluster carrier)
    into the legacy cluster-dict shape that the rest of optimizer.py
    consumes. Mirrors :meth:`LlmCluster.to_legacy_dict` field-for-field
    so downstream consumers (proposal shaping, lever dispatch,
    postmortem rendering) need no Plan 11 awareness.

    Plan 11 specifics: ``repair_hypothesis`` (free text) is stamped on
    a same-named key; ``suggested_repair_shape`` is set to
    ``"other"`` (the legacy ``RepairShape.OTHER`` value, deliberately
    the LLM's free-form escape hatch — see ``repair_intent.py``).
    ``source`` is ``"llm_plan11"`` so postmortems can distinguish
    Plan 11 clusters from legacy Plan 4 LLM clusters.
    """
    return {
        "cluster_id": str(fc.cluster_id),
        "question_ids": list(fc.member_qids),
        "asi_blame_set": list(fc.primary_blame_set),
        "asi_blame_set_normalized": list(fc.primary_blame_set),
        "root_cause": str(fc.semantic_theme),
        "asi_failure_type": str(fc.semantic_theme),
        "failure_keys": [
            str(fc.semantic_theme),
            str(fc.repair_hypothesis),
        ],
        "semantic_theme": str(fc.semantic_theme),
        "suggested_repair_shape": "other",
        "repair_hypothesis": str(fc.repair_hypothesis),
        "llm_confidence": str(fc.confidence),
        "llm_rationale": str(fc.unifying_evidence),
        "source": "llm_plan11",
        "signal_type": str(signal_type),
    }


_RCA_SQL_SNIPPET_PATCH_TYPES: frozenset[str] = frozenset({
    "add_sql_snippet_measure",
    "add_sql_snippet_filter",
    "add_sql_snippet_expression",
})


def _rca_themes_requesting_sql_snippets(
    themes: list[Any],
    target_qids: Iterable[str] | None = None,
) -> list[Any]:
    out: list[Any] = []
    for theme in themes or []:
        if not _theme_matches_target_qids(theme, target_qids):
            continue
        patches = getattr(theme, "patches", None)
        if isinstance(theme, dict):
            patches = theme.get("patches")
        for patch in patches or ():
            if isinstance(patch, dict) and patch.get("type") in _RCA_SQL_SNIPPET_PATCH_TYPES:
                out.append(theme)
                break
    return out


def _rca_themes_requesting_join_specs(
    themes: list[Any],
    target_qids: Iterable[str] | None = None,
) -> list[Any]:
    out: list[Any] = []
    for theme in themes or []:
        if not _theme_matches_target_qids(theme, target_qids):
            continue
        patches = getattr(theme, "patches", None)
        if isinstance(theme, dict):
            patches = theme.get("patches")
        for patch in patches or ():
            if isinstance(patch, dict) and patch.get("type") == "add_join_spec":
                out.append(theme)
                break
    return out


_RCA_LEVER1_PATCH_TYPES: frozenset[str] = frozenset({
    "update_column_description",
    "add_column_synonym",
    "update_description",
})


def _lever1_theme_key(cluster: dict[str, Any]) -> tuple[str, str, tuple[str, ...]]:
    """Stable theme key for grouping Lever-1 RCA work.

    Tuple of (root_cause, patch_family, sorted_blame_set). Two clusters
    with the same key are in the same RCA theme and can share metadata
    proposals so we don't ask the LLM to rewrite the same description
    once per cluster.
    """
    blame = cluster.get("asi_blame_set") or cluster.get("blame_set") or []
    if isinstance(blame, str):
        blame_items = [blame]
    else:
        blame_items = [str(item) for item in blame]
    return (
        str(cluster.get("root_cause") or "unknown"),
        str(cluster.get("patch_family") or "unknown"),
        tuple(sorted(item for item in blame_items if item)),
    )


def _rca_themes_requesting_lever1(
    themes: list[Any],
    target_qids: Iterable[str] | None = None,
) -> list[Any]:
    out: list[Any] = []
    for theme in themes or []:
        if not _theme_matches_target_qids(theme, target_qids):
            continue
        patches = getattr(theme, "patches", None)
        if isinstance(theme, dict):
            patches = theme.get("patches")
        for patch in patches or ():
            if isinstance(patch, dict) and patch.get("type") in _RCA_LEVER1_PATCH_TYPES:
                out.append(theme)
                break
    return out


def _format_rca_themes_for_strategy(
    rca_themes: list[Any],
    conflicts: list[Any],
) -> str:
    lines: list[str] = ["## Typed RCA Themes"]
    if not rca_themes:
        lines.append("(No typed RCA themes available.)")
    for idx, theme in enumerate((rca_themes or [])[:8], 1):
        rca_id = _field(theme, "rca_id", "")
        rca_kind = _field(theme, "rca_kind", "")
        if hasattr(rca_kind, "value"):
            rca_kind = rca_kind.value
        patch_family = _field(theme, "patch_family", "")
        target_qids = _field(theme, "target_qids", ())
        touched = _field(theme, "touched_objects", ())
        confidence = _field(theme, "confidence", 0.0)
        evidence = str(_field(theme, "evidence_summary", "") or "")
        patches = list(_field(theme, "patches", ()) or ())
        levers = sorted({
            int(p.get("lever"))
            for p in patches
            if isinstance(p, dict) and str(p.get("lever", "")).isdigit()
        })
        lines.append(
            f"{idx}. {rca_id} kind={rca_kind} family={patch_family} "
            f"confidence={float(confidence):.2f} recommended_levers={levers} "
            f"targets={list(target_qids)} touched={list(touched)[:8]}"
        )
        if evidence:
            lines.append(f"   evidence={evidence[:500]}")
        for patch in patches[:6]:
            if not isinstance(patch, dict):
                continue
            ptype = patch.get("type", "")
            lever = patch.get("lever", "")
            intent = patch.get("intent", "")
            target = (
                patch.get("target")
                or patch.get("target_object")
                or patch.get("column")
                or patch.get("table")
                or patch.get("root_cause")
                or ""
            )
            lines.append(
                f"   - lever={lever} patch_type={ptype} target={target} "
                f"intent={str(intent)[:240]}"
            )
    if conflicts:
        lines.append("\n## RCA Theme Conflict Matrix")
        for conflict in conflicts[:8]:
            left = _field(conflict, "left_rca_id", "")
            right = _field(conflict, "right_rca_id", "")
            obj = _field(conflict, "object_id", "")
            reason = _field(conflict, "reason", "")
            lines.append(f"- {left} -> {right} on `{obj}`: {reason}")
    return "\n".join(lines)


def _build_context_data(
    *,
    clusters: list[dict],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    reflection_buffer: list[dict],
    priority_ranking: list[dict],
    blame_set: list[str] | None,
    success_summary: str,
    reflection_text: str,
    persistence_text: str,
    proven_patterns_text: str,
    suggestions_text: str,
    iq_scan_text: str = "",
    rca_theme_context: str = "",
) -> dict:
    """Assemble all context sections as a single Python dict for JSON serialization."""
    from genie_space_optimizer.optimization.applier import _get_general_instructions
    from genie_space_optimizer.common.config import (
        unresolved_target_debt_strategist_enabled,
    )

    relevant_tables = _extract_tables_from_clusters(clusters + soft_signal_clusters)

    context: dict = {
        "progress_summary": success_summary,
        "mandatory_regression_debt_qids": (
            list(metadata_snapshot.get("_mandatory_regression_debt_qids") or [])
            or None
        ),
        "priority_analysis": [
            {
                "rank": c.get("rank", "?"),
                "cluster_id": c.get("cluster_id", "?"),
                "root_cause": c.get("root_cause", "?"),
                "judge": c.get("affected_judge", "?"),
                "questions": len(c.get("question_ids", [])),
                "impact": c.get("impact_score", 0),
            }
            for c in priority_ranking[:10]
        ],
        "reflection_history": reflection_text,
        "proven_patterns": proven_patterns_text,
        "persistent_failures": persistence_text,
        "human_suggestions": suggestions_text or None,
        "iq_scan_findings": iq_scan_text or None,
        "rca_theme_context": rca_theme_context or None,
        "schema": _build_schema_data(metadata_snapshot, filter_tables=relevant_tables),
        "failure_clusters": _build_cluster_data(clusters),
        "failure_features": [
            {
                "cluster_id": c.get("cluster_id"),
                **(c.get("failure_features") or {}),
            }
            for c in clusters[:10]
            if c.get("failure_features")
        ],
        "soft_signal_clusters": _build_soft_signal_data(soft_signal_clusters),
        "structured_metadata": {
            "tables": _build_structured_table_data(metadata_snapshot, blame_set),
            "columns": _build_structured_column_data(metadata_snapshot, blame_set),
            "functions": _build_structured_function_data(metadata_snapshot),
        },
        "join_specifications": _build_join_specs_data(metadata_snapshot),
        "current_instructions": _get_general_instructions(metadata_snapshot) or None,
        "existing_example_sqls": _build_example_sqls_data(metadata_snapshot),
        "blamed_column_values": _build_blamed_values_data(
            clusters, metadata_snapshot.get("_data_profile", {}),
        ),
    }

    # Cycle 14-C T6: include the unresolved-target-debt slot only
    # when (a) the flag is on AND (b) there is actual debt to
    # surface. Empty / absent debt means no slot — keeps the
    # strategist prompt clean and byte-stable on flag-off.
    if unresolved_target_debt_strategist_enabled():
        debt = list(
            metadata_snapshot.get("_unresolved_target_debt_qids") or []
        )
        if debt:
            context["unresolved_target_debt_qids"] = debt

    return context


_SQL_PATTERN_ROOT_CAUSES = frozenset({
    "wrong_table", "wrong_join", "missing_filter", "missing_aggregation",
    "wrong_aggregation", "wrong_measure", "select_star", "tvf_parameter_error",
})


_SQL_SHAPE_ROOT_CAUSES = frozenset({
    # Superset of _SQL_PATTERN_ROOT_CAUSES used by A3 (Lever 5 structural
    # gate) and B2 (weighted root-cause tie-break). A "SQL-shape" cause is
    # any failure whose fix requires changing the generated SQL's
    # structure (tables, joins, filters, measures, dimensions) rather
    # than prose instructions. For these causes, a Lever 5 text_instruction
    # is a weak signal; we require an example_sql or route to a different
    # lever (6, 4, 3, 1).
    "wrong_table",
    "wrong_column",
    "wrong_join",
    "wrong_join_spec",
    "missing_join_spec",
    "missing_filter",
    "missing_scd_filter",
    "missing_temporal_filter",
    "wrong_filter_condition",
    "wrong_aggregation",
    "wrong_measure",
    "missing_aggregation",
    "missing_dimension",
    "wrong_grouping",
    "select_star",
    "tvf_parameter_error",
    # Tier 2.13 / 2.14: Genie behaviour patterns detected by the
    # over_filtered_dimension / wide_vs_long_shape classifiers in
    # evaluation.classify_genie_shape_patterns. Treated as SQL-shape
    # because the fix is an example_sql showing the correct row shape,
    # not a prose instruction.
    "over_filtered_dimension",
    "wide_vs_long_shape",
    # P1 pattern labels emitted by ``_detect_failure_pattern`` (this file,
    # near L990+). Each pattern's corrective fix is an ``example_sql``
    # demonstrating the right SQL shape — never a prose instruction —
    # so they belong here so the Lever 5 structural gate (A3a/A3b) blocks
    # text-only proposals and forces example_sql synthesis.
    "plural_top_n_collapse",
    "time_window_pivot",
    "value_format_mismatch",
    "column_disambiguation",
    "granularity_drop",
})


def _ag_structural_root_causes_for_clusters(
    *,
    source_clusters: list[str],
    clusters_by_id: dict,
) -> set[str]:
    """Phase 6.3 (2026-05-17) — return the set of SQL-shape root
    causes across an AG's source clusters.

    Previously inlined at the L5 structural gate (around lines
    16271-16282). The pre-Phase-6 logic used Python's ``or`` short-
    circuit::

        _rc = (_c.get("asi_failure_type") or _c.get("root_cause") or "")

    which silently dropped the legitimate ``root_cause`` whenever
    ``asi_failure_type`` was truthy but non-SQL-shape (the Run B H002
    regression: ``asi_failure_type="other"``,
    ``root_cause="plural_top_n_collapse"`` — gate missed the
    SQL-shape label).

    The fix evaluates BOTH labels via :func:`cluster_failure_keys`
    (which already returns both, de-duplicated, with empties
    filtered) and set-intersects against
    :data:`_SQL_SHAPE_ROOT_CAUSES`.
    """
    # Plan 11: cluster_failure_keys is a pure read-only utility shared
    # by both the legacy archetype path and the Plan 11 LLM-first
    # path; plan11_llm_first_enabled() does not change behavior here.
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        cluster_failure_keys,
    )
    rcs: set[str] = set()
    for sid in (source_clusters or ()):
        cluster = clusters_by_id.get(sid)
        if not isinstance(cluster, dict):
            continue
        for key in cluster_failure_keys(cluster):
            if key in _SQL_SHAPE_ROOT_CAUSES:
                rcs.add(key)
    return rcs


_ACTIONABLE_ROOT_CAUSES: frozenset[str] = _SQL_SHAPE_ROOT_CAUSES | frozenset({
    "column_disambiguation",
    "missing_filter",
    "missing_temporal_filter",
    "missing_join_spec",
    "missing_scd_filter",
    "wrong_filter_condition",
})


def _select_dominant_root_cause(weighted: dict[str, float]) -> str:
    """Return the dominant root cause with actionability as the primary key.

    Actionable labels are members of ``_ACTIONABLE_ROOT_CAUSES`` — the
    SQL-shape causes plus explicit filter/join/disambiguation labels.
    Non-actionable labels (notably ``format_difference`` and
    ``extra_columns_only``) reflect cosmetic output differences the
    optimizer cannot ship as a lever-shaped fix and therefore lose to
    any actionable label even at lower weight.
    """
    if not weighted:
        return "other"
    return max(
        weighted.items(),
        key=lambda kv: (
            1 if kv[0] in _ACTIONABLE_ROOT_CAUSES else 0,
            kv[1],
            1 if kv[0] in _SQL_SHAPE_ROOT_CAUSES else 0,
            -len(kv[0]),
        ),
    )[0]


# ── Bug #4 (benchmark leakage) counters ────────────────────────────────
# Incremented whenever the optimizer suppresses a path that would have copied
# benchmark content verbatim. Harvested by write_iteration() and reset per
# iteration via reset_bug4_counters().
_BUG4_COUNTERS: dict[str, int] = {
    "secondary_mining_blocked": 0,
    "firewall_rejections": 0,
    # Phase A3: bumped when a Lever 5 instruction-only proposal is blocked
    # because the cluster's dominant root cause is structural
    # (in _SQL_SHAPE_ROOT_CAUSES) but no example_sql is attached.
    "lever5_text_only_blocked": 0,
}


def reset_bug4_counters() -> None:
    """Reset Bug #4 counters. Called at the start of each iteration."""
    for key in _BUG4_COUNTERS:
        _BUG4_COUNTERS[key] = 0


def get_bug4_counters() -> dict[str, int]:
    """Snapshot of Bug #4 counters for persistence by write_iteration()."""
    return dict(_BUG4_COUNTERS)


def _incr_bug4_counter(key: str, amount: int = 1) -> None:
    _BUG4_COUNTERS[key] = _BUG4_COUNTERS.get(key, 0) + amount


# ── Lever 5 structural-gate drop side-channel ──────────────────────────
# Cycle 8 Bug 1 Phase 3b Task B: the gate at line 13961 silently zeroes
# instruction proposals when the dominant cluster root cause is SQL-shape
# but no example_sql is attached. We append one record per drop here so
# the harness can build typed DecisionRecords downstream and surface the
# drop in the operator transcript. Reset per iteration alongside the
# Bug-4 counters.
_LEVER5_GATE_DROPS: list[dict] = []


def reset_lever5_gate_drops() -> None:
    """Reset the Lever 5 structural-gate drop ledger between iterations."""
    _LEVER5_GATE_DROPS.clear()


def get_lever5_gate_drops() -> list[dict]:
    """Snapshot of Lever 5 structural-gate drops for harness wiring."""
    return list(_LEVER5_GATE_DROPS)


def _resolve_lever5_llm_result(
    llm_result: dict, original_patch_type: str, cluster: dict | None = None,
) -> tuple[str, dict]:
    """Interpret the instruction_type returned by the Lever 6 LLM and resolve
    the actual patch_type and extra fields to merge into the proposal.

    When the cluster's root_cause is a clear SQL-pattern issue (e.g.
    ``wrong_join``, ``missing_filter``) and the cluster has SQL context with
    a valid expected SQL, force ``add_example_sql`` to provide a concrete
    pattern rather than a verbose text instruction.

    Returns ``(resolved_patch_type, extra_fields)``.
    """
    instruction_type = llm_result.get("instruction_type", "text_instruction")

    if instruction_type == "example_sql":
        return "add_example_sql", {
            "example_question": llm_result.get("example_question", ""),
            "example_sql": llm_result.get("example_sql", ""),
            "parameters": llm_result.get("parameters", []),
            "usage_guidance": llm_result.get("usage_guidance", ""),
        }

    if instruction_type == "sql_expression":
        snippet_type = llm_result.get("snippet_type", "measure")
        patch_type_map = {
            "measure": "add_sql_snippet_measure",
            "filter": "add_sql_snippet_filter",
            "expression": "add_sql_snippet_expression",
        }
        resolved_type = patch_type_map.get(snippet_type, "add_sql_snippet_measure")
        logger.info(
            "Lever 5 LLM recommended sql_expression — routing to Lever 6 "
            "(type=%s, table=%s)",
            snippet_type, llm_result.get("target_table", ""),
        )
        return resolved_type, {
            "snippet_type": snippet_type,
            "display_name": llm_result.get("display_name", ""),
            "alias": llm_result.get("alias", ""),
            "sql": llm_result.get("sql", ""),
            "synonyms": llm_result.get("synonyms", []),
            "instruction": llm_result.get("instruction", ""),
            "target_table": llm_result.get("target_table", ""),
        }

    # Bug #4 — benchmark leakage prevention. Previously, when the LLM
    # returned text_instruction for a SQL-pattern root cause, this block would
    # copy representative["question"]/representative["expected_sql"] verbatim
    # into an add_example_sql proposal. That channel is closed: it contaminated
    # the training signal by installing benchmark SQL into the space being
    # evaluated on those same benchmarks. Structural synthesis (Phase 3) is
    # the supported replacement. We still record that we blocked this path so
    # observability can confirm the fix is active.
    if cluster and cluster.get("root_cause") in _SQL_PATTERN_ROOT_CAUSES:
        sql_ctxs = cluster.get("sql_contexts", [])
        representative = next(
            (sc for sc in sql_ctxs if sc.get("expected_sql") and sc.get("question")),
            None,
        )
        if representative:
            _incr_bug4_counter("secondary_mining_blocked")
            logger.info(
                "Bug #4: secondary mining blocked for SQL-pattern root cause "
                "'%s' — falling through to text_instruction (was: verbatim "
                "copy of benchmark question/expected_sql)",
                cluster["root_cause"],
            )
            # Fall through to the structural-gate check below (A3b).

    # Phase A3b: structural-cause gate. For the broader
    # _SQL_SHAPE_ROOT_CAUSES set, a text-only instruction is a weak
    # signal — we drop it with a sentinel instead of emitting a
    # downgraded add_instruction. The caller is expected to recognize
    # the sentinel and skip the proposal. Bug #4's counter bump above
    # is preserved so observability parity is maintained for the
    # narrower _SQL_PATTERN_ROOT_CAUSES overlap.
    if cluster:
        _cluster_rc = (
            cluster.get("asi_failure_type")
            or cluster.get("root_cause")
            or ""
        )
        if _cluster_rc in _SQL_SHAPE_ROOT_CAUSES:
            logger.info(
                "Phase A3b: Lever 5 text_instruction skipped for structural "
                "root cause '%s' — expected example_sql or different lever.",
                _cluster_rc,
            )
            return "skipped_no_example_sql", {
                "reason": (
                    "text_instruction is too weak a signal for structural "
                    "root causes; an example_sql or structural lever is "
                    "required."
                ),
                "root_cause": _cluster_rc,
            }

    if original_patch_type == "add_example_sql":
        logger.warning(
            "Lever 6 LLM returned text_instruction for a routing failure "
            "(original_patch_type=add_example_sql). Example SQL is preferred "
            "for routing issues. Keeping text_instruction but marking as downgraded."
        )

    raw_text = llm_result.get("instruction_text", "")
    return "add_instruction", {
        "new_text": _sanitize_plaintext_instructions(raw_text) if raw_text else "",
        "downgraded_from_example_sql": original_patch_type == "add_example_sql",
    }


def _format_raw_evidence_block(raw_evidence: tuple[dict, ...]) -> str:
    """Plan 4 — render raw evidence for the ``{{ raw_evidence_block }}``
    prompt slot.

    Empty tuple → ``"(No raw failure evidence — proceed using AFS only.)"``.
    Non-empty tuple → anti-anchoring header from
    ``common.config._RAW_EVIDENCE_ANTI_ANCHORING_HEADER`` followed by
    one ``Example k of N`` block per triple. Each SQL / rationale
    field is truncated to 600 chars to keep prompt budget bounded.
    """
    if not raw_evidence:
        return "(No raw failure evidence — proceed using AFS only.)"
    from genie_space_optimizer.common.config import (
        _RAW_EVIDENCE_ANTI_ANCHORING_HEADER,
    )
    parts: list[str] = [_RAW_EVIDENCE_ANTI_ANCHORING_HEADER, ""]
    n = len(raw_evidence)
    for i, t in enumerate(raw_evidence, start=1):
        parts.append(
            f"Example {i} of {n} — qid={t.get('question_id', '')}\n"
            f"  question: {t.get('question', '')}\n"
            f"  actual_sql:\n{(t.get('actual_sql', '') or '')[:600]}\n"
            f"  expected_sql:\n{(t.get('expected_sql', '') or '')[:600]}\n"
            f"  judge_rationale: {(t.get('judge_rationale', '') or '')[:600]}"
        )
    return "\n\n".join(parts)


def _lever_1_2_format_kwargs(
    *,
    cluster: dict,
    metadata_snapshot: dict,
    lever: int,
    raw_evidence: tuple[dict, ...] = (),
) -> dict[str, Any]:
    """Build the format_kwargs for the L1/L2 column-refinement prompt.

    Returns ONLY the 9 slots the LEVER_1_2_COLUMN_PROMPT template
    references. Prunes the 17 dead-weight keys the shared
    _call_llm_for_proposal historically supplied (failures_context,
    current_join_specs, table_names, etc.) — they were carried over
    from levers 3/4/5 and the L1/L2 template never used them.

    Plan: 2026-05-17-lever-1-2-column-prompt-hardening.md Task 8
    """
    from genie_space_optimizer.optimization.afs import format_afs

    _afs = format_afs(cluster)
    sql_diffs = json.dumps(_afs.get("structural_diff", {}), default=str)

    blame = cluster.get("asi_blame_set")
    if not blame:
        blame = _derive_blame_from_sql(cluster)

    # Blame-scoped allowlist (Task 9 — restricts identifier set to objects
    # relevant to this cluster's failure, reducing prompt token cost).
    _relevant_objects: set[str] = set(blame) if blame else set()
    for entry in list(_relevant_objects):
        if isinstance(entry, str) and entry.count(".") >= 3:
            parent_table = ".".join(entry.split(".")[:3])
            _relevant_objects.add(parent_table)
    _allowlist = _build_identifier_allowlist(
        metadata_snapshot,
        relevant_objects=_relevant_objects or None,
    )

    return {
        "failure_type": cluster.get(
            "asi_failure_type", cluster.get("root_cause", "")
        ),
        "blame_set": blame or "",
        "affected_questions": cluster.get("question_ids", []),
        "counterfactual_fixes": cluster.get("asi_counterfactual_fixes", []),
        "raw_evidence_block": _format_raw_evidence_block(raw_evidence),
        "sql_diffs": sql_diffs,
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
        "structured_column_context": _format_structured_column_context(
            metadata_snapshot, blame, lever,
        ),
        "structured_table_context": _format_structured_table_context(
            metadata_snapshot, blame, lever,
        ),
    }


def _call_llm_for_proposal(
    cluster: dict,
    metadata_snapshot: dict,
    patch_type: str,
    lever: int,
    w: WorkspaceClient | None = None,
    *,
    raw_evidence: tuple[dict, ...] = (),
    response_model: type | None = None,
) -> dict:
    """Call Databricks Claude Opus 4.6 to generate proposal text.

    Returns ``{"proposed_value": str, "rationale": str}``.
    For lever 5 the response may also contain ``instruction_type``,
    ``example_question``, ``example_sql``, ``target_table``, etc.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions

    # Plan 2026-05-17-lever-1-2-column-prompt-hardening Task 8 — L1/L2
    # fast path. Builds ONLY the 9 slots the LEVER_1_2_COLUMN_PROMPT
    # template actually references (drops 17 dead-weight keys carried
    # over from levers 3/4/5). Uses the L1/L2-specific system message
    # and explicit max_tokens. Routes through _traced_llm_call only when
    # a response_model is supplied — otherwise stays on the legacy
    # _call_llm_openai path so existing tests that mock the OpenAI
    # client at the llm_client layer continue to work.
    if lever in (1, 2):
        from genie_space_optimizer.common.config import (
            LEVER_1_2_MAX_TOKENS,
            LEVER_1_2_SYSTEM_MSG,
        )
        from genie_space_optimizer.optimization.evaluation import (
            _extract_json,
            _link_prompt_to_trace,
        )

        format_kwargs = _lever_1_2_format_kwargs(
            cluster=cluster,
            metadata_snapshot=metadata_snapshot,
            lever=lever,
            raw_evidence=raw_evidence,
        )
        prompt = format_mlflow_template(LEVER_1_2_COLUMN_PROMPT, **format_kwargs)
        _link_prompt_to_trace("lever_1_2_column")

        if response_model is not None:
            text, _response = _traced_llm_call(
                w,
                LEVER_1_2_SYSTEM_MSG,
                prompt,
                span_name=f"lever_{lever}_column_proposal",
                max_tokens=LEVER_1_2_MAX_TOKENS,
                response_model=response_model,
            )
        else:
            text, _response = _call_llm_openai(
                w,
                messages=[
                    {"role": "system", "content": LEVER_1_2_SYSTEM_MSG},
                    {"role": "user", "content": prompt},
                ],
                max_retries=LLM_MAX_RETRIES,
                temperature=LLM_TEMPERATURE,
                max_tokens=LEVER_1_2_MAX_TOKENS,
            )
        parsed = _extract_json(text)
        if isinstance(parsed, dict):
            return parsed
        return {"raw_text": text}

    prompt_map = {
        1: LEVER_1_2_COLUMN_PROMPT,
        2: LEVER_1_2_COLUMN_PROMPT,
        # LEVER_4_JOIN_SPEC_PROMPT is deprecated (see
        # _DEPRECATED_PROMPT_NAMES in common/config.py) — superseded by
        # LEVER_4_JOIN_DISCOVERY_PROMPT and only reachable via this
        # legacy holistic-proposal path which is itself deprecated.
        4: LEVER_4_JOIN_SPEC_PROMPT,
        5: LEVER_5_INSTRUCTION_PROMPT,
    }
    prompt_template = prompt_map.get(lever, PROPOSAL_GENERATION_PROMPT)

    current_dict_count = sum(
        1
        for t in metadata_snapshot.get("tables", [])
        for c in t.get("column_configs", [])
        if c.get("enable_entity_matching")
    )

    # Bug #4 (P2.2) — Route cluster context through the AFS serializer so no
    # raw benchmark text (question / expected_sql / generated_sql) reaches the
    # prompt. The AFS output is a typed, leak-free classification; the legacy
    # ``_format_sql_diffs`` carries raw SQL tokens and is reserved for
    # debug-only logging (behind GSO_DEBUG_RAW_SQL=1, see P2.6).
    from genie_space_optimizer.optimization.afs import format_afs
    _afs = format_afs(cluster)
    sql_diffs = json.dumps(_afs.get("structural_diff", {}), default=str)
    blame = cluster.get("asi_blame_set")
    if not blame:
        blame = _derive_blame_from_sql(cluster)

    existing_example_sqls = _format_existing_example_sqls(metadata_snapshot)

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _inst_lookup = metadata_snapshot.get("instructions", {})
    if not isinstance(_inst_lookup, dict):
        _inst_lookup = {}
    _tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    _mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])
    _funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])
    _join_specs = (
        metadata_snapshot.get("join_specs", [])
        or _inst_lookup.get("join_specs", [])
        or ds.get("join_specs", [])
    )

    _allowlist = _build_identifier_allowlist(metadata_snapshot)

    format_kwargs: dict[str, Any] = {
        "failure_type": cluster.get("asi_failure_type", cluster.get("root_cause", "")),
        "blame_set": blame or "",
        "affected_questions": cluster.get("question_ids", []),
        "counterfactual_fixes": cluster.get("asi_counterfactual_fixes", []),
        "severity": "major",
        "current_metadata": _extract_metadata_for_blame(
            metadata_snapshot, blame
        ),
        "patch_type_description": _describe_patch_type(patch_type),
        # Bug #4 (P2.2) — failures_context is the AFS projection, never the
        # raw cluster dict. Prevents question/expected_sql/generated_sql +
        # result samples from reaching the LLM prompt.
        "failures_context": json.dumps(_afs, default=str),
        "sql_diffs": sql_diffs,
        "current_join_specs": json.dumps(_join_specs, default=str),
        "table_relationships": json.dumps(
            [t.get("relationships", []) for t in _tables],
            default=str,
        ),
        "current_column_configs": json.dumps(
            metadata_snapshot.get("column_configs", {}), default=str
        ),
        "full_schema_context": _format_full_schema_context(metadata_snapshot),
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
        "string_column_count": metadata_snapshot.get("string_column_count", 0),
        "max_value_dictionary_cols": MAX_VALUE_DICTIONARY_COLUMNS,
        "current_dictionary_count": current_dict_count,
        "current_instructions": _get_general_instructions(metadata_snapshot),
        "existing_example_sqls": existing_example_sqls,
        "instruction_char_budget": max(
            0,
            24500 - len(_get_general_instructions(metadata_snapshot)),
        ),
        "table_names": [
            t.get("name") or t.get("identifier", "")
            for t in _tables
        ],
        "mv_names": [
            m.get("name") or m.get("identifier", "")
            for m in _mvs
        ],
        "tvf_names": [
            f.get("name") or f.get("identifier", "")
            for f in _funcs
        ],
        "raw_evidence_block": _format_raw_evidence_block(raw_evidence),
    }

    if lever in (1, 2):
        format_kwargs["structured_column_context"] = _format_structured_column_context(
            metadata_snapshot, blame, lever,
        )
        format_kwargs["structured_table_context"] = _format_structured_table_context(
            metadata_snapshot, blame, lever,
        )
        format_kwargs["full_schema_context"] = "(See structured table/column metadata above for relevant schema.)"
        format_kwargs.pop("current_column_configs", None)

    format_kwargs = _truncate_to_budget(
        format_kwargs, prompt_template,
        priority_keys=["full_schema_context", "current_column_configs", "table_relationships", "failures_context", "sql_diffs"],
    )

    prompt = format_mlflow_template(prompt_template, **format_kwargs)

    from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
    _tmpl_name = {1: "LEVER_1_2_COLUMN", 2: "LEVER_1_2_COLUMN", 4: "LEVER_4_JOIN_SPEC", 5: "LEVER_5_INSTRUCTION"}.get(lever, "PROPOSAL_GENERATION")
    _link_prompt_to_trace(_tmpl_name.lower())
    _W = 78
    _hdr = f"┌─── LLM Call [{_tmpl_name}] " + "─" * max(0, _W - 18 - len(_tmpl_name))
    _ftr = "└" + "─" * (_W - 1)

    _cid = cluster.get("cluster_id", "?")
    _root = cluster.get("root_cause", "?")
    _q_traces = cluster.get("question_traces", [])
    _ctxts = cluster.get("sql_contexts", [])

    _judge_lines = []
    for qt in _q_traces[:5]:
        for jt in qt.get("failed_judges", []):
            snip = (jt.get("rationale_snippet") or "")[:120].replace("\n", " ")
            _judge_lines.append(f"│   {qt['question_id'][:40]} / {jt['judge']}: \"{snip}\"")

    _sql_diff_lines = []
    if _ctxts:
        ctx = _ctxts[0]
        exp_snip = (ctx.get("expected_sql") or "")[:200].replace("\n", " ")
        gen_snip = (ctx.get("generated_sql") or "")[:200].replace("\n", " ")
        if exp_snip or gen_snip:
            _sql_diff_lines.append(f"│   Expected:  {exp_snip}")
            _sql_diff_lines.append(f"│   Generated: {gen_snip}")

    _cfix_lines = []
    for cf in cluster.get("asi_counterfactual_fixes", [])[:3]:
        _cfix_lines.append(f"│   \"{str(cf)[:120]}\"")

    _extra = (
        f"│ {'Cluster:':<24s} {_cid}\n"
        f"│ {'Root cause:':<24s} {_root}\n"
    )
    if _judge_lines:
        _extra += "│\n│ --- Judge Feedback Driving This Patch ---\n" + "\n".join(_judge_lines) + "\n"
    if _sql_diff_lines:
        _extra += "│\n│ --- SQL Diff (sample) ---\n" + "\n".join(_sql_diff_lines) + "\n"
    if _cfix_lines:
        _extra += "│\n│ --- Counterfactual Fixes ---\n" + "\n".join(_cfix_lines) + "\n"

    print(
        f"\n{_hdr}\n"
        f"│ {'Patch type:':<24s} {patch_type}\n"
        f"│ {'Failure type:':<24s} {format_kwargs.get('failure_type', '?')}\n"
        f"│ {'Blame set:':<24s} {format_kwargs.get('blame_set', '?')}\n"
        f"│ {'Questions:':<24s} {len(format_kwargs.get('affected_questions', []))}\n"
        f"{_extra}"
        f"│ {'Prompt length:':<24s} {len(prompt):,} chars\n│"
    )

    import time

    from genie_space_optimizer.optimization.evaluation import _extract_json

    _proposal_system_msg = (
        "You are a JSON-only responder. Your ENTIRE response must be a single "
        "valid JSON object. Do not include any analysis, markdown, commentary, "
        "or explanation outside the JSON. Start your response with '{' and end "
        "with '}'."
    )

    text = ""
    for attempt in range(LLM_MAX_RETRIES):
        try:
            # Plan 2026-05-17-prompt-registry-and-typed-io-hygiene Task 13 —
            # when response_model is provided, route through
            # _traced_llm_call (which auto-wires response_format and
            # server-side JSON-schema enforcement). Otherwise preserve the
            # legacy _call_llm_openai path so existing callers are
            # unaffected.
            if response_model is not None:
                text, _response = _traced_llm_call(
                    w, _proposal_system_msg, prompt,
                    span_name=f"call_llm_for_proposal_lever_{lever}",
                    max_retries=1,
                    temperature=LLM_TEMPERATURE,
                    response_model=response_model,
                )
            else:
                text, _response = _call_llm_openai(
                    w,
                    messages=[
                        {"role": "system", "content": _proposal_system_msg},
                        {"role": "user", "content": prompt},
                    ],
                    max_retries=1,
                    temperature=LLM_TEMPERATURE,
                )
            parsed = _extract_json(text)
            print(
                f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} OK -- parsed JSON\n"
                f"│ {'Proposed value:':<24s} {str(parsed.get('proposed_value', ''))[:300]}\n"
                f"│ {'Rationale:':<24s} {str(parsed.get('rationale', ''))[:300]}\n"
                f"│ {'Result:':<24s} OK\n"
                f"{_ftr}"
            )
            return parsed
        except json.JSONDecodeError:
            if attempt < LLM_MAX_RETRIES - 1:
                print(f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} non-JSON (retrying...)")
                time.sleep(2**attempt)
                continue
            print(
                f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} non-JSON -- retries exhausted\n"
                f"│ Raw text (500 chars): {text[:500]}\n"
                f"│\n"
                f"│ {'Result:':<24s} FALLBACK (raw text kept as proposed_value)\n"
                f"{_ftr}"
            )
            return {"proposed_value": text, "rationale": "LLM response was not valid JSON after all retries"}
        except Exception:
            if attempt < LLM_MAX_RETRIES - 1:
                print(f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} error (retrying...)")
                time.sleep(2**attempt)
            else:
                logger.exception("LLM [%s] call failed after %d retries", _tmpl_name, LLM_MAX_RETRIES)
                print(
                    f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} error -- retries exhausted\n"
                    f"│ {'Result:':<24s} FAILED\n"
                    f"{_ftr}"
                )
                return {
                    "proposed_value": "",
                    "rationale": "LLM call failed",
                }
    return {
        "proposed_value": "",
        "rationale": "LLM call failed",
    }


def _format_discovery_hints(
    hints: list[dict],
    join_overlaps: list[dict] | None = None,
) -> str:
    """Format discovery hints into a human-readable string for the LLM prompt.

    When *join_overlaps* are available (from preflight data profiling), each
    hint is annotated with the FK overlap ratio for the relevant table pair.
    """
    if not hints:
        return "(no heuristic hints)"

    overlap_index: dict[tuple[str, str], dict] = {}
    for ov in (join_overlaps or []):
        key = (ov.get("left_table", "").lower(), ov.get("right_table", "").lower())
        overlap_index[key] = ov
        overlap_index[(key[1], key[0])] = ov

    lines: list[str] = []
    for idx, h in enumerate(hints, 1):
        lt = h.get("left_table", "")
        rt = h.get("right_table", "")
        compat = h.get("type_compatible", True)
        lines.append(f"### Hint {idx}: {lt} ↔ {rt}")
        if not compat:
            lines.append("  ⚠️  Some candidate columns have mismatched types")

        ov = overlap_index.get((lt.lower(), rt.lower()))
        if ov:
            ratio = ov.get("overlap_ratio", 0)
            fk_col = ov.get("fk_column", "?")
            pk_col = ov.get("pk_column", "?")
            pct = f"{ratio * 100:.0f}%"
            lines.append(
                f"  📊 FK overlap: {fk_col} → {pk_col} = {pct} "
                f"({ov.get('left_distinct', '?')} distinct FK values)"
            )

        for cc in h.get("candidate_columns", []):
            lines.append(
                f"  - {cc.get('left_col', '?')} ↔ {cc.get('right_col', '?')} "
                f"({cc.get('reason', 'unknown')})"
            )
    return "\n".join(lines)


_JOIN_PROSE_SPLIT_RE = re.compile(
    r",\s*(?:MANY_TO_|ONE_TO_|Use this|This join|Always|Note:)",
    re.IGNORECASE,
)
_SENTENCE_BOUNDARY_RE = re.compile(r"\.\s+[A-Z]")


_EQUIJOIN_PREDICATE_RE = re.compile(
    r"`?[\w.]+`?\s*\.\s*`?\w+`?\s*=\s*`?[\w.]+`?\s*\.\s*`?\w+`?",
)


def _extract_equijoin_predicates(sql_str: str) -> str:
    """Keep only ``table.col = table.col`` predicates from a join SQL string.

    The Genie API ``join_specs[].sql`` field only accepts equijoin predicates
    (column equality between two tables).  LLMs and execution-proven SQL
    sometimes include filter predicates such as
    ``AND dim_property.is_current = true`` which the API rejects.

    This function extracts all ``a.x = b.y`` predicates and returns them
    joined by `` AND ``, discarding everything else.
    """
    predicates = _EQUIJOIN_PREDICATE_RE.findall(sql_str)
    return " AND ".join(predicates)


def _sanitize_join_sql(sql_str: str) -> str:
    """Strip prose, cardinality labels, and non-equijoin predicates.

    LLMs sometimes embed descriptive text like ``MANY_TO_ONE. Use this join
    to connect...`` after the actual ON-clause expression.  This function
    first strips prose, then extracts only equijoin predicates that the
    Genie API accepts.
    """
    cleaned = _JOIN_PROSE_SPLIT_RE.split(sql_str, maxsplit=1)[0]
    cleaned = _SENTENCE_BOUNDARY_RE.split(cleaned, maxsplit=1)[0]
    cleaned = cleaned.strip().rstrip(",").rstrip(".")
    equijoin_only = _extract_equijoin_predicates(cleaned)
    return equijoin_only if equijoin_only else cleaned


# Plan 2026-05-17-lever-4-join-discovery-hardening.md Task 6 —
# trim the verbose JSON-API guard now that typed-IO (response_model)
# enforces the JSON contract upstream. v2 is the active version;
# system_msg_version="v2" is emitted as an observability tag.
_LEVER_4_SYSTEM_MSG_VERSION = "v2"
_LEVER_4_SYSTEM_MSG = (
    "You are a Databricks Genie Space join-relationship expert. "
    "Return only the join_specs JSON requested by the prompt."
)


def _normalize_instruction(value: Any) -> tuple[str, bool, bool]:
    """Coerce + sanitize an L4 instruction field at the boundary.

    Plan 2026-05-17-lever-4-join-discovery-hardening.md Task 10.

    Returns (normalized, changed, truncated) where:
      - normalized: the canonical str shape
      - changed: True if input shape required coercion (list -> str)
                 or sanitization (markdown stripped)
      - truncated: True if output was truncated past
                   LEVER_4_INSTRUCTION_SOFT_CAP
    """
    from genie_space_optimizer.common.config import (
        LEVER_4_INSTRUCTION_SOFT_CAP,
    )

    changed = False
    if isinstance(value, list):
        value = "\n".join(str(x) for x in value if x is not None)
        changed = True
    elif value is None:
        return "", True, False
    elif not isinstance(value, str):
        value = str(value)

    original = value
    value = re.sub(r"^```[a-z]*\s*\n", "", value)
    value = re.sub(r"\n```\s*$", "", value)
    value = re.sub(r"\*\*([^*]+)\*\*", r"\1", value)
    value = value.strip()
    if value != original:
        changed = True

    truncated = False
    if len(value) > LEVER_4_INSTRUCTION_SOFT_CAP:
        value = value[:LEVER_4_INSTRUCTION_SOFT_CAP] + " […truncated]"
        truncated = True

    return value, changed, truncated


def _normalize_join_endpoint(
    ep: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Derive alias from identifier; override any model-provided value.

    Plan 2026-05-17-lever-4-join-discovery-hardening.md Task 11.

    Returns (normalized, overridden) where:
      - normalized: dict with canonical alias
      - overridden: True if the model's alias did NOT match the
                    derivation (signal for prompt-tuning)
    """
    if not isinstance(ep, dict):
        return ep, False
    identifier = ep.get("identifier", "")
    if not isinstance(identifier, str) or not identifier:
        return ep, False
    canonical_alias = identifier.split(".")[-1]
    model_alias = ep.get("alias")
    out = dict(ep)
    out["alias"] = canonical_alias
    return out, (model_alias != canonical_alias)


def _format_existing_join_specs_compact(specs: list[dict]) -> str:
    """Render existing join_specs as 1 line per spec.

    Plan 2026-05-17-lever-4-join-discovery-hardening.md Task 12.
    Token-efficient vs json.dumps for large spaces.

    Format: ``<l_alias> <-> <r_alias> on <predicate> (rt=<sentinel>)``
    """
    if not specs:
        return "(none)"
    lines: list[str] = []
    for s in specs:
        if not isinstance(s, dict):
            continue
        left = (s.get("left") or {}) if isinstance(s.get("left"), dict) else {}
        right = (
            (s.get("right") or {}) if isinstance(s.get("right"), dict) else {}
        )
        l_alias = (
            left.get("alias")
            or left.get("identifier", "?").split(".")[-1]
        )
        r_alias = (
            right.get("alias")
            or right.get("identifier", "?").split(".")[-1]
        )
        sql_parts = s.get("sql") or []
        predicate = ""
        rt = ""
        for part in sql_parts:
            if not isinstance(part, str):
                continue
            m = re.search(r"--rt=(FROM_RELATIONSHIP_TYPE_[A-Z_]+)--", part)
            if m:
                rt = m.group(1)
            elif "=" in part and "--rt=" not in part:
                predicate = part.strip()
        lines.append(f"{l_alias} <-> {r_alias} on {predicate} (rt={rt})")
    return "\n".join(lines) if lines else "(none)"


def _cap_discovery_hints(hints: list[dict]) -> tuple[list[dict], bool]:
    """Cap heuristic hint count at LEVER_4_HINTS_TOP_K.

    Plan 2026-05-17-lever-4-join-discovery-hardening.md Task 13.
    """
    from genie_space_optimizer.common.config import LEVER_4_HINTS_TOP_K

    if len(hints) <= LEVER_4_HINTS_TOP_K:
        return hints, False
    return hints[:LEVER_4_HINTS_TOP_K], True


def _l4_response_appears_truncated(text: str) -> bool:
    """Heuristic: detect mid-spec truncation in an L4 LLM response.

    Plan 2026-05-17-lever-4-join-discovery-hardening.md Task 14.
    """
    if not isinstance(text, str):
        return False
    stripped = text.strip()
    if stripped.startswith("```"):
        if "\n" in stripped:
            stripped = stripped.split("\n", 1)[1]
    stripped = stripped.strip("`").strip()
    if not stripped.endswith("}") and not stripped.endswith("]"):
        return True
    opens = stripped.count("{")
    closes = stripped.count("}")
    if opens > closes:
        return True
    return False


def _repair_l4_truncated_json(
    w: Any,
    system_msg: str,
    partial_text: str,
) -> str | None:
    """Request a single-shot continuation to recover truncated output.

    Plan 2026-05-17-lever-4-join-discovery-hardening.md Task 14.
    Asks the model for the missing trailing content needed to complete
    the JSON object it already started, then concatenates with the
    partial response. Returns ``None`` on repair failure so the caller
    can degrade gracefully (the existing `_extract_json` fallback path
    still applies).
    """
    from genie_space_optimizer.common.config import LEVER_4_MAX_TOKENS

    repair_prompt = (
        "Your previous response was truncated mid-way. Return ONLY the "
        "missing trailing content needed to complete the JSON object you "
        "started below. Do not repeat any content that is already present.\n\n"
        "Partial response so far:\n"
        + partial_text[-2000:]
    )
    try:
        text, _ = _traced_llm_call(
            w, system_msg, repair_prompt,
            span_name="lever_4_join_discovery_repair",
            max_tokens=LEVER_4_MAX_TOKENS,
        )
    except Exception:
        logger.warning(
            "L4 repair _traced_llm_call failed; returning None",
            exc_info=True,
        )
        return None
    if not isinstance(text, str):
        return None
    return partial_text + text


def _set_l4_observability_tags(
    span: Any,
    *,
    prompt_version: str = "v1",
    system_msg_version: str = "v1",
    pydantic_validation_status: str = "unknown",
    markdown_fence_stripped: bool = False,
    instruction_coerced_to_string: bool = False,
    input_truncated: bool = False,
    sanitize_made_changes: bool = False,
    mv_join_emitted: bool = False,
    existing_specs_rendered_chars: int = 0,
    hints_truncated: bool = False,
    raw_evidence_block_version: str = "v1",
    repair_used: bool = False,
    rca_contract_version: str = "non_causal_v1",
    alias_overridden: bool = False,
    relationship_type_invalid: bool = False,
) -> None:
    """Emit the closed set of L4 join-discovery observability tags.

    Plan 2026-05-17-lever-4-join-discovery-hardening.md Task 1.
    Single point of MLflow tag emission for lever_4_join_discovery.
    Adding a new tag requires extending
    ``LEVER_4_OBSERVABILITY_TAG_KEYS`` (config.py) and the
    ``test_l4_observability_tag_keys_are_closed`` regression guard.

    Tagging is best-effort: failures here MUST NOT break the call
    path. The ``span`` argument is allowed to be ``None`` for the
    no-MLflow code paths (legacy test stubs).
    """
    if span is None:
        return
    tags = {
        "prompt_version": prompt_version,
        "system_msg_version": system_msg_version,
        "pydantic_validation_status": pydantic_validation_status,
        "markdown_fence_stripped": str(markdown_fence_stripped).lower(),
        "instruction_coerced_to_string": str(instruction_coerced_to_string).lower(),
        "input_truncated": str(input_truncated).lower(),
        "sanitize_made_changes": str(sanitize_made_changes).lower(),
        "mv_join_emitted": str(mv_join_emitted).lower(),
        "existing_specs_rendered_chars": str(existing_specs_rendered_chars),
        "hints_truncated": str(hints_truncated).lower(),
        "raw_evidence_block_version": raw_evidence_block_version,
        "repair_used": str(repair_used).lower(),
        "rca_contract_version": rca_contract_version,
        "alias_overridden": str(alias_overridden).lower(),
        "relationship_type_invalid": str(relationship_type_invalid).lower(),
    }
    try:
        for k, v in tags.items():
            span.set_attribute(f"l4.{k}", v)
    except Exception:
        # Best-effort: tracing failures must not break L4 dispatch.
        logger.debug("L4 observability tag emission failed", exc_info=True)


def _call_llm_for_join_discovery(
    metadata_snapshot: dict,
    hints: list[dict],
    w: WorkspaceClient | None = None,
    *,
    raw_evidence: tuple[dict, ...] = (),
) -> list[dict]:
    """Call the LLM with the discovery prompt to validate and refine join hints.

    Returns a list of ``{"join_spec": {...}, "rationale": str}`` dicts.
    """
    from genie_space_optimizer.optimization.evaluation import _extract_json

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _inst_js = metadata_snapshot.get("instructions", {})
    if not isinstance(_inst_js, dict):
        _inst_js = {}
    _join_specs = (
        metadata_snapshot.get("join_specs", [])
        or _inst_js.get("join_specs", [])
        or ds.get("join_specs", [])
    )

    # Task 13: cap hints at top-K so the <discovery_hints> block stays bounded.
    hints, _hints_truncated = _cap_discovery_hints(hints)

    hint_tables: set[str] = set()
    for h in hints:
        for k in ("left_table", "right_table", "table", "source_table", "target_table"):
            v = h.get(k)
            if v and isinstance(v, str):
                hint_tables.add(v.lower())

    if hint_tables:
        scoped_schema = _format_full_schema_context(metadata_snapshot, filter_tables=hint_tables)
    else:
        scoped_schema = _format_full_schema_context(metadata_snapshot)

    _allowlist = _build_identifier_allowlist(metadata_snapshot)

    # Task 12: compact current_join_specs rendering replaces raw json.dumps.
    _compact_specs = _format_existing_join_specs_compact(_join_specs)
    format_kwargs: dict[str, Any] = {
        "full_schema_context": scoped_schema,
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
        "current_join_specs": _compact_specs,
        "discovery_hints": _format_discovery_hints(
            hints,
            join_overlaps=metadata_snapshot.get("_join_overlaps"),
        ),
        "raw_evidence_block": _format_raw_evidence_block(raw_evidence),
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, LEVER_4_JOIN_DISCOVERY_PROMPT,
        priority_keys=["full_schema_context", "current_join_specs"],
    )

    prompt = format_mlflow_template(LEVER_4_JOIN_DISCOVERY_PROMPT, **format_kwargs)

    import mlflow
    from mlflow.entities import SpanType

    with mlflow.start_span(
        name="lever_4_join_discovery",
        span_type=SpanType.CHAIN,
    ) as _span:
        _span.set_inputs({
            "model": LLM_ENDPOINT,
            "temperature": LLM_TEMPERATURE,
            "prompt_chars": len(prompt),
            "hints": len(hints),
            "existing_join_specs": len(_join_specs),
        })
        # Task 1: emit the full L4 tag suite with defaults. Subsequent
        # tasks replace defaults with computed values as they land.
        _set_l4_observability_tags(_span)

        from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
        _link_prompt_to_trace("lever_4_join_discovery")

        logger.info(
            "\n"
            "┌─── OPTIMIZER LLM [JOIN_DISCOVERY] INPUT ────────────────────────────────\n"
            "│ Hints: %d\n"
            "│ Existing join specs: %d\n"
            "│ Prompt length: %d chars\n"
            "└─────────────────────────────────────────────────────────────────────────",
            len(hints), len(_join_specs), len(prompt),
        )

        # Task 6: trimmed system message (v2). Typed-IO upstream enforces
        # the JSON contract — the verbose JSON-API guard was redundant.
        system_msg = _LEVER_4_SYSTEM_MSG

        from genie_space_optimizer.optimization.prompt_io import (
            Lever4JoinDiscoveryOutput,
        )
        from genie_space_optimizer.common.config import LEVER_4_MAX_TOKENS

        _pydantic_status = "ok"
        try:
            text, _response = _traced_llm_call(
                w, system_msg, prompt,
                span_name="lever_4_join_discovery",
                max_tokens=LEVER_4_MAX_TOKENS,
                response_model=Lever4JoinDiscoveryOutput,
            )
        except Exception:
            logger.warning(
                "Lever-4 join discovery LLM call failed after retries",
                exc_info=True,
            )
            _set_l4_observability_tags(
                _span, pydantic_validation_status="llm_call_failed",
            )
            _span.set_outputs({"join_specs_returned": 0, "error": True})
            return []

        # Task 4: prefer the validated Pydantic model over re-parsing
        # raw text. Falls back to _extract_json only if the response
        # was non-conformant.
        _repair_used = False
        if isinstance(_response, Lever4JoinDiscoveryOutput):
            result = _response.model_dump()
        else:
            _pydantic_status = "bypass_used"
            result = None

            # Task 14: detect mid-spec truncation BEFORE falling back
            # to _extract_json. If the response looks cut off, ask the
            # model for the missing trailing content, concatenate, and
            # retry Pydantic validation. The bypass path still applies
            # if repair fails.
            if _l4_response_appears_truncated(text):
                repaired_text = _repair_l4_truncated_json(
                    w, system_msg, text,
                )
                _repair_used = True
                if repaired_text:
                    try:
                        repaired_model = (
                            Lever4JoinDiscoveryOutput.model_validate_json(
                                repaired_text,
                            )
                        )
                        result = repaired_model.model_dump()
                        _pydantic_status = "ok_after_repair"
                    except Exception:
                        logger.debug(
                            "L4 repair succeeded but Pydantic validation "
                            "still failed; falling back to _extract_json",
                            exc_info=True,
                        )
                        # Reassign so the bypass path has the repaired
                        # payload to grep through.
                        text = repaired_text

            if result is None:
                try:
                    result = _extract_json(text) or {}
                    if not isinstance(result, dict):
                        result = {}
                except json.JSONDecodeError:
                    logger.warning(
                        "JOIN_DISCOVERY non-JSON response: %.500s", text,
                    )
                    _set_l4_observability_tags(
                        _span,
                        pydantic_validation_status="bypass_failed",
                        repair_used=_repair_used,
                    )
                    _span.set_outputs(
                        {"join_specs_returned": 0, "non_json": True},
                    )
                    return []
        specs = result.get("join_specs", []) or []
        rationale = result.get("rationale", "") or ""

        # Tasks 10 + 11 — boundary normalization per spec.
        _any_instruction_changed = False
        _any_instruction_truncated = False
        _any_alias_overridden = False
        _any_mv_join_emitted = False
        for s in specs:
            if not isinstance(s, dict):
                continue
            # Task 10: instruction shape + soft cap
            raw_instr = s.get("instruction", "")
            norm_instr, instr_changed, instr_truncated = _normalize_instruction(
                raw_instr,
            )
            s["instruction"] = norm_instr
            _any_instruction_changed = (
                _any_instruction_changed or instr_changed
            )
            _any_instruction_truncated = (
                _any_instruction_truncated or instr_truncated
            )
            # Task 11: alias derivation per endpoint
            for side in ("left", "right"):
                ep = s.get(side, {})
                norm_ep, alias_overridden = _normalize_join_endpoint(ep)
                s[side] = norm_ep
                _any_alias_overridden = (
                    _any_alias_overridden or alias_overridden
                )
            # MV-join detection: an emitted spec touches a metric view
            # when either endpoint identifier starts with mv_ (last
            # segment heuristic mirrors the prompt rule).
            for side in ("left", "right"):
                ep = s.get(side, {}) if isinstance(s.get(side), dict) else {}
                ident = str(ep.get("identifier") or "")
                last = ident.split(".")[-1] if ident else ""
                if last.startswith("mv_"):
                    _any_mv_join_emitted = True
                    break

        _any_sanitize_changes = False
        for s in specs:
            if isinstance(s, dict) and "sql" in s:
                original_sql = list(s.get("sql") or [])
                s["sql"] = [
                    _sanitize_join_sql(part) for part in s["sql"]
                    if isinstance(part, str)
                ]
                if s["sql"] != original_sql:
                    _any_sanitize_changes = True
        out = [
            {"join_spec": s, "rationale": rationale}
            for s in specs
            if isinstance(s, dict)
        ]

        # Final tag emission with computed values from this call. The
        # earlier _set_l4_observability_tags(_span) at span entry seeded
        # defaults; here we overwrite with the real values.
        _set_l4_observability_tags(
            _span,
            prompt_version="v1",
            system_msg_version=_LEVER_4_SYSTEM_MSG_VERSION,
            pydantic_validation_status=_pydantic_status,
            instruction_coerced_to_string=_any_instruction_changed,
            input_truncated=_any_instruction_truncated,
            sanitize_made_changes=_any_sanitize_changes,
            mv_join_emitted=_any_mv_join_emitted,
            existing_specs_rendered_chars=len(_compact_specs),
            hints_truncated=_hints_truncated,
            alias_overridden=_any_alias_overridden,
            repair_used=_repair_used,
        )
        _span.set_outputs({"join_specs_returned": len(out)})
        logger.info(
            "\n"
            "┌─── OPTIMIZER LLM [JOIN_DISCOVERY] RESPONSE ─────────────────────────\n"
            "│ Join specs returned: %d\n"
            "│ Rationale: %s\n"
            "└─────────────────────────────────────────────────────────────────────────",
            len(out), _truncate_on_boundary(str(rationale), 300),
        )
        return out
        _span.set_outputs({"join_specs_returned": 0, "exhausted": True})
        return []


_MARKDOWN_RESIDUE_RE = re.compile(
    r'(?m)'
    r'(?:^```[a-z]*\s*$)'                     # fenced code blocks
    r'|(?:^---+\s*$)'                         # horizontal rules
    r'|(?:^\*\*\*+\s*$)'
    r'|(?:^___+\s*$)'
    r'|(?:^#{1,6}\s+\S)'                      # leading ``## HEADER``
    r'|(?:\*\*[^*]+\*\*)'                     # bold
    r'|(?:`[^`]+`)'                           # inline backticks
    r'|(?:\[[^\]]+\]\([^)]+\))'               # markdown links
    r'|(?:\n{3,})'                            # excess blank lines
)


def _is_already_canonical_plaintext(text: str) -> bool:
    """Phase 3.3: cheap idempotency check for the sanitizer.

    Returns True if *text* contains no Markdown residue that
    :func:`_sanitize_plaintext_instructions` would otherwise touch. We
    skip the regex pipeline in that case so a second pass over already-
    canonical input doesn't generate spurious diffs (re-stripping
    backticks, re-flowing whitespace) on every iteration.
    """
    if not text:
        return True
    return _MARKDOWN_RESIDUE_RE.search(text) is None


def _sanitize_plaintext_instructions(text: str) -> str:
    """Strip residual Markdown from instruction text for plain-text display.

    Phase 3.3: idempotent — if the input already has no Markdown
    residue, the function returns the (stripped) text unchanged
    instead of running the regex pipeline. This eliminates the
    "every iteration produces a diff for unchanged content" symptom
    from the iter-1 lever loop.
    """
    if _is_already_canonical_plaintext(text):
        return text.strip() if text else ""
    text = re.sub(r'^```[a-z]*\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^---+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^\*\*\*+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^___+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(
        r'^#{1,6}\s+(.+)$',
        lambda m: m.group(1).upper().rstrip() + ':',
        text,
        flags=re.MULTILINE,
    )
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'\*(.+?)\*', r'\1', text)
    text = re.sub(r'`([^`]+)`', r'\1', text)
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


_SECTION_HEADER_RE = re.compile(
    r'^([A-Z][A-Z /]+):[ \t]*$', re.MULTILINE,
)
_KNOWN_SECTIONS = set(INSTRUCTION_SECTION_ORDER)

_INLINE_SECTION_NAMES: list[str] = sorted(
    INSTRUCTION_SECTION_ORDER, key=lambda s: len(s), reverse=True
)
_INLINE_SECTION_RE = re.compile(
    r'(?:^|\s)('
    + '|'.join(re.escape(s) for s in _INLINE_SECTION_NAMES)
    + r'):\s*'
)


def _parse_sections(text: str) -> tuple[dict[str, list[str]], list[str]]:
    """Parse structured plain-text into {SECTION_HEADER: [lines]} and preamble lines."""
    lines = text.splitlines()
    sections: dict[str, list[str]] = {}
    preamble: list[str] = []
    current: str | None = None

    for line in lines:
        m = _SECTION_HEADER_RE.match(line)
        if m and m.group(1) in _KNOWN_SECTIONS:
            current = m.group(1)
            if current not in sections:
                sections[current] = []
        elif current is not None:
            sections[current].append(line)
        else:
            preamble.append(line)

    for key in sections:
        while sections[key] and not sections[key][-1].strip():
            sections[key].pop()

    return sections, preamble


def _merge_structured_instructions(
    existing: str,
    contributions: list[str],
    global_guidance: str = "",
) -> str:
    """Merge instruction fragments into a single structured document.

    Parses ``existing`` and each contribution by ALL-CAPS section header,
    deduplicates bullets within each section, then reassembles in
    ``INSTRUCTION_SECTION_ORDER``.  Unrecognized content goes into CONSTRAINTS.
    """
    merged: dict[str, list[str]] = {s: [] for s in INSTRUCTION_SECTION_ORDER}

    existing_sections, existing_preamble = _parse_sections(
        _sanitize_plaintext_instructions(existing) if existing else ""
    )
    for section, lines in existing_sections.items():
        if section in merged:
            merged[section].extend(lines)

    if existing_preamble:
        non_blank = [l for l in existing_preamble if l.strip()]
        if non_blank:
            if not merged["PURPOSE"]:
                merged["PURPOSE"].extend(non_blank)
            else:
                merged["CONSTRAINTS"].extend(non_blank)

    for fragment in contributions:
        sanitized = _sanitize_plaintext_instructions(fragment) if fragment else ""
        frag_sections, frag_preamble = _parse_sections(sanitized)
        for section, lines in frag_sections.items():
            if section in merged:
                merged[section].extend(lines)
            else:
                merged["CONSTRAINTS"].extend(lines)
        if frag_preamble:
            non_blank = [l for l in frag_preamble if l.strip()]
            if non_blank:
                merged["CONSTRAINTS"].extend(non_blank)

    if global_guidance:
        sanitized_g = _sanitize_plaintext_instructions(global_guidance)
        g_sections, g_preamble = _parse_sections(sanitized_g)
        for section, lines in g_sections.items():
            if section in merged:
                merged[section].extend(lines)
        if g_preamble:
            non_blank = [l for l in g_preamble if l.strip()]
            if non_blank:
                merged["CONSTRAINTS"].extend(non_blank)

    for section in merged:
        seen: set[str] = set()
        deduped: list[str] = []
        for line in merged[section]:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped not in seen:
                seen.add(stripped)
                deduped.append(line)
        merged[section] = deduped

    parts: list[str] = []
    for section in INSTRUCTION_SECTION_ORDER:
        if merged[section]:
            parts.append(f"{section}:")
            for line in merged[section]:
                stripped = line.strip()
                if not stripped:
                    continue
                if not stripped.startswith("- "):
                    stripped = f"- {stripped}"
                parts.append(stripped)
            parts.append("")

    result = "\n".join(parts).strip()
    return _sanitize_plaintext_instructions(result)


def normalize_instructions(text: str) -> str:
    """Parse text into canonical structured sections and reassemble."""
    return _merge_structured_instructions(existing=text, contributions=[], global_guidance="")


# ---------------------------------------------------------------------------
# Instruction pre-structuring: convert unstructured instructions into
# canonical ALL-CAPS sections so section-level merges are safe.
# ---------------------------------------------------------------------------

_pre_structure_cache: dict[int, dict[str, list[str]]] = {}


def _is_unstructured(text: str) -> bool:
    """Return True if *text* has no recognized section headers.

    Treats both the legacy 12-section ALL-CAPS vocabulary AND the new
    canonical 5-section schema (PR #178) as "structured" — the lever
    loop's restructure block must not re-classify a space whose prose
    has already been normalised by the prose rule miner. That would
    undo the miner's work on every optimisation run and produce ever-
    growing churn in the text_instructions content.
    """
    if not text or not text.strip():
        return True
    # New canonical-schema detection runs BEFORE sanitisation because
    # ``_sanitize_plaintext_instructions`` rewrites ``## FOO`` to
    # ``FOO:`` (legacy shape). If we let it through first, the verbatim
    # header #5 loses its casing and passes the lever loop's ALL-CAPS
    # detector only accidentally.
    from genie_space_optimizer.common.config import CANONICAL_SECTION_HEADERS
    if any(h in text for h in CANONICAL_SECTION_HEADERS):
        return False
    sanitized = _sanitize_plaintext_instructions(text)
    sections, preamble = _parse_sections(sanitized)
    return (not sections) and bool(preamble)


def _pre_structure_instructions(
    raw: str,
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict[str, list[str]]:
    """Heuristic fallback that groups free-form prose into legacy sections.

    Historical note: this function used to drive an LLM round-trip via
    ``INSTRUCTION_RESTRUCTURE_PROMPT`` to classify prose into the legacy
    12-section ALL-CAPS vocabulary. That prompt and the classifier were
    deleted as part of the 5-section schema migration — the prose rule
    miner (:func:`_convert_instructions_to_sql_expressions`) now performs
    canonical grouping as part of its rewrite step, which subsumes this
    concern for spaces touched by proactive enrichment.

    What remains is a heuristic fallback used by the in-loop lever
    machinery (:func:`_ensure_structured`) when a space's prose still
    lacks recognised section headers. The fallback preserves content
    rather than inventing structure; the miner re-homes it on the next
    optimisation run.
    """
    if not raw or not raw.strip():
        return {}

    cache_key = hash(raw)
    if cache_key in _pre_structure_cache:
        return _pre_structure_cache[cache_key]

    fallback_text = _merge_structured_instructions(
        existing=raw, contributions=[], global_guidance="",
    )
    sections, preamble = _parse_sections(fallback_text)
    if preamble:
        non_blank = [ln for ln in preamble if ln.strip()]
        if non_blank:
            target = "PURPOSE" if "PURPOSE" not in sections else "CONSTRAINTS"
            sections.setdefault(target, []).extend(non_blank)
    if not sections:
        # Nothing parseable — dump under CONSTRAINTS so content is
        # preserved, not silently dropped. The miner will promote or
        # re-home it on the next run.
        sections = {
            "CONSTRAINTS": [
                ln.strip() for ln in raw.splitlines() if ln.strip()
            ],
        }
    result: dict[str, list[str]] = {
        k: [ln for ln in v if ln.strip()]
        for k, v in sections.items()
    }
    _pre_structure_cache[cache_key] = result
    return result


def _ensure_structured(
    current_instructions: str,
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict[str, list[str]]:
    """Return existing instructions as a structured section dict.

    If already structured, parses directly.  If unstructured, calls
    ``_pre_structure_instructions`` to classify via LLM.
    """
    if not current_instructions or not current_instructions.strip():
        return {}

    sanitized = _sanitize_plaintext_instructions(current_instructions)
    sections, preamble = _parse_sections(sanitized)

    if sections and not preamble:
        return {k: [ln for ln in v if ln.strip()] for k, v in sections.items()}

    if sections and preamble:
        non_blank = [ln for ln in preamble if ln.strip()]
        if non_blank:
            target = "PURPOSE" if "PURPOSE" not in sections else "CONSTRAINTS"
            sections.setdefault(target, []).extend(non_blank)
        return {k: [ln for ln in v if ln.strip()] for k, v in sections.items()}

    return _pre_structure_instructions(current_instructions, metadata_snapshot, w=w)


# ---------------------------------------------------------------------------
# Semantic content-preservation check
# ---------------------------------------------------------------------------

_KEY_PHRASE_RE = re.compile(
    r'[a-z_]+\.[a-z_]+\.[a-z_]+'    # 3-part identifiers
    r'|[A-Z]{2,}[A-Z_]*'              # ALL-CAPS acronyms (2+ chars)
    r'|[a-z_]{2,}\.[a-z_]{2,}'        # 2-part identifiers
    r"|'[YN]'"                         # flag literals
)


def _extract_key_phrases(text: str) -> set[str]:
    """Extract domain-significant tokens from instruction text."""
    phrases: set[str] = set()
    for m in _KEY_PHRASE_RE.finditer(text):
        phrases.add(m.group(0))
    return phrases


def _instruction_coverage(old: str, new: str, threshold: float = 0.6) -> bool:
    """Check that key identifiers from *old* appear in *new*.

    Returns ``True`` if coverage is above *threshold* (i.e. the new text
    preserves enough of the original's key phrases).
    """
    old_tokens = _extract_key_phrases(old)
    if not old_tokens:
        return True
    new_tokens = _extract_key_phrases(new)
    coverage = len(old_tokens & new_tokens) / len(old_tokens)
    if coverage < threshold:
        logger.warning(
            "Instruction coverage %.1f%% < %.0f%% threshold — "
            "%d/%d key phrases missing: %s",
            coverage * 100, threshold * 100,
            len(old_tokens - new_tokens), len(old_tokens),
            sorted(old_tokens - new_tokens)[:10],
        )
    return coverage >= threshold


def _repair_truncated_holistic_json(text: str) -> dict:
    """Extract instruction_text and example_sql_proposals from a truncated JSON.

    When the LLM output exceeds max_tokens, the JSON is cut off mid-string.
    Attempts to salvage both ``instruction_text`` and ``example_sql_proposals``.
    """
    instruction_text = ""
    example_sql_proposals: list[dict] = []

    m = re.search(r'"instruction_text"\s*:\s*"', text)
    if m:
        start = m.end()
        i = start
        while i < len(text):
            ch = text[i]
            if ch == "\\" and i + 1 < len(text):
                i += 2
                continue
            if ch == '"':
                break
            i += 1
        else:
            i = len(text)
        raw = text[start:i]
        try:
            instruction_text = json.loads(f'"{raw}"')
        except (json.JSONDecodeError, ValueError):
            instruction_text = raw.replace('\\"', '"').replace("\\n", "\n")

    m_ex = re.search(r'"example_sql_proposals"\s*:\s*\[', text)
    if m_ex:
        bracket_start = m_ex.end() - 1
        depth = 0
        for j in range(bracket_start, len(text)):
            if text[j] == "[":
                depth += 1
            elif text[j] == "]":
                depth -= 1
            if depth == 0:
                try:
                    example_sql_proposals = json.loads(text[bracket_start : j + 1])
                except json.JSONDecodeError:
                    pass
                break

    if not instruction_text and not example_sql_proposals:
        raise json.JSONDecodeError("No instruction_text or example_sql_proposals found", text, 0)

    logger.warning(
        "Repaired truncated holistic JSON — %d chars instruction, %d example SQL proposals",
        len(instruction_text), len(example_sql_proposals),
    )
    return {
        "instruction_text": instruction_text,
        "example_sql_proposals": example_sql_proposals,
        "rationale": "Recovered from truncated JSON response",
    }


def _repair_truncated_l5a_json(text: str) -> dict:
    """Extract instruction_text + rationale from a truncated L5a JSON.

    The L5a output schema is Lever5aInstructionsOutput
    (prompt_io.py) with ONLY two fields. This function differs
    from _repair_truncated_holistic_json in that it does NOT look
    for or include example_sql_proposals — that field belongs to a
    different schema and including it would cause the downstream
    no-SQL gate to false-positive.

    Added per 2026-05-17-lever-5a-instructions-hardening.md Task 5
    (baseline §6.A2).
    """
    instruction_text = ""
    rationale = ""

    # Extract instruction_text via regex (handles truncation mid-string)
    m_instr = re.search(
        r'"instruction_text"\s*:\s*"((?:[^"\\]|\\.)*)',
        text, re.DOTALL,
    )
    if m_instr:
        raw = m_instr.group(1)
        instruction_text = (
            raw.replace('\\"', '"').replace('\\n', '\n').replace('\\\\', '\\')
        )

    # Extract rationale via regex (may also be truncated)
    m_rat = re.search(
        r'"rationale"\s*:\s*"((?:[^"\\]|\\.)*)',
        text, re.DOTALL,
    )
    if m_rat:
        raw = m_rat.group(1)
        rationale = (
            raw.replace('\\"', '"').replace('\\n', '\n').replace('\\\\', '\\')
        )

    if not instruction_text:
        logger.warning(
            "L5a JSON repair found no instruction_text in truncated "
            "response (len=%d): %.200s",
            len(text), text,
        )

    return {"instruction_text": instruction_text, "rationale": rationale}


def _call_llm_for_holistic_instructions(
    all_clusters: list[dict],
    metadata_snapshot: dict,
    lever_changes: list[dict] | None = None,
    w: WorkspaceClient | None = None,
) -> dict:
    """Single LLM call to synthesize ALL evaluation learnings into holistic instructions.

    Returns ``{"instruction_text": str, "example_sql_proposals": list, "rationale": str}``.

    DEPRECATED 2026-05-17 (Plan 2026-05-17-lever-5a-instructions-
    hardening.md Task 16, baseline §6.E1). The L5 holistic path was
    superseded by ``_dispatch_lever_5_split`` (Plan 2; unconditionally
    on as of 2026-05-16). No production caller invokes this function
    today; it is retained for one release for emergency rollback +
    test_llm_client.py exercise. To remove permanently, delete this
    function, the ``LEVER_5_HOLISTIC_PROMPT`` constant, the
    ``LEVER_PROMPTS["lever_5_holistic"]`` registry entry, and update
    the test in tests/unit/test_llm_client.py.
    """
    logger.warning(
        "DEPRECATED: _call_llm_for_holistic_instructions invoked. "
        "Use _dispatch_lever_5_split instead. Tracked by "
        "2026-05-17-lever-5a-instructions-hardening.md Task 16."
    )
    from genie_space_optimizer.optimization.applier import _get_general_instructions

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    _mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])
    _funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])

    config = metadata_snapshot.get("config") or {}
    space_desc = config.get("description") or ""
    if isinstance(space_desc, list):
        space_desc = "\n".join(space_desc)
    if not space_desc:
        space_desc = "(No description set for this Genie Space.)"

    current_instructions = _get_general_instructions(metadata_snapshot)
    existing_example_sqls = _format_existing_example_sqls(metadata_snapshot)

    resolved_ids: set[str] = set()
    for lc in (lever_changes or []):
        for cid in (lc.get("cluster_ids", []) or []):
            if lc.get("status") in ("applied", "success"):
                resolved_ids.add(str(cid))

    unresolved = [
        c for c in all_clusters
        if str(c.get("cluster_id", "")) not in resolved_ids
    ]
    focus_clusters = unresolved if unresolved else all_clusters

    _allowlist = _build_identifier_allowlist(metadata_snapshot)

    format_kwargs: dict[str, Any] = {
        "space_description": space_desc,
        "eval_summary": _format_eval_summary(focus_clusters),
        "cluster_briefs": _format_cluster_briefs_afs(focus_clusters, top_n=5),
        "lever_summary": _format_lever_summary(lever_changes),
        "current_instructions": current_instructions or "(No current instructions.)",
        "existing_example_sqls": existing_example_sqls,
        "instruction_char_budget": max(0, 24500 - 500),
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
        "table_names": [t.get("name") or t.get("identifier", "") for t in _tables],
        "mv_names": [m.get("name") or m.get("identifier", "") for m in _mvs],
        "tvf_names": [f.get("name") or f.get("identifier", "") for f in _funcs],
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, LEVER_5_HOLISTIC_PROMPT,
        priority_keys=["existing_example_sqls", "lever_summary", "cluster_briefs", "eval_summary"],
    )

    prompt = format_mlflow_template(LEVER_5_HOLISTIC_PROMPT, **format_kwargs)

    from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
    _link_prompt_to_trace("lever_5_holistic")

    _W = 78
    _hdr = "┌─── LLM Call [LEVER_5_HOLISTIC] " + "─" * max(0, _W - 32)
    _ftr = "└" + "─" * (_W - 1)

    logger.info(
        "\n%s\n│ Clusters: %d\n│ Lever changes: %d\n│ Prompt length: %d chars\n%s",
        _hdr, len(focus_clusters), len(lever_changes or []), len(prompt), _ftr,
    )

    import time

    from genie_space_optimizer.optimization.evaluation import _extract_json

    holistic_system_msg = (
        "You are a JSON API. You MUST respond with ONLY a valid JSON object. "
        "Do NOT include any explanation, analysis, or markdown outside the JSON. "
        "Your entire response must be parseable by json.loads(). "
        "The JSON must contain an 'instruction_text' string field."
    )

    text = ""
    for attempt in range(LLM_MAX_RETRIES):
        try:
            text, _response = _call_llm_openai(
                w,
                messages=[
                    {"role": "system", "content": holistic_system_msg},
                    {"role": "user", "content": prompt},
                ],
                max_retries=1,
                temperature=LLM_TEMPERATURE,
            )
            try:
                result = _extract_json(text)
            except json.JSONDecodeError:
                result = _repair_truncated_holistic_json(text)

            instruction_text = result.get("instruction_text", "")
            if instruction_text:
                instruction_text = _sanitize_plaintext_instructions(instruction_text)
            example_proposals = result.get("example_sql_proposals", [])
            rationale = result.get("rationale", "")

            if instruction_text and len(instruction_text) > MAX_HOLISTIC_INSTRUCTION_CHARS:
                logger.warning(
                    "Holistic instruction text exceeds %d chars (%d), truncating",
                    MAX_HOLISTIC_INSTRUCTION_CHARS, len(instruction_text),
                )
                instruction_text = instruction_text[:MAX_HOLISTIC_INSTRUCTION_CHARS]

            logger.info(
                "\n┌─── LLM Response [LEVER_5_HOLISTIC] ──────────────────────────────────\n"
                "│ Instruction text: %d chars\n"
                "│ Example SQL proposals: %d\n"
                "│ Rationale: %s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                len(instruction_text), len(example_proposals), _truncate_on_boundary(str(rationale), 300),
            )
            return {
                "instruction_text": instruction_text,
                "example_sql_proposals": example_proposals if isinstance(example_proposals, list) else [],
                "rationale": rationale,
            }
        except json.JSONDecodeError:
            logger.warning(
                "Holistic LLM response was not valid JSON (attempt %d): %.500s",
                attempt + 1, text,
            )
            if attempt >= LLM_MAX_RETRIES - 1:
                m_regex = re.search(r'"instruction_text"\s*:\s*"(.{50,}?)"', text, re.DOTALL)
                if m_regex:
                    recovered = m_regex.group(1).replace('\\"', '"').replace("\\n", "\n")
                    logger.info("Last-ditch regex recovered %d chars of instruction_text", len(recovered))
                    return {"instruction_text": recovered, "example_sql_proposals": [], "rationale": "Regex recovery"}
                return {"instruction_text": "", "example_sql_proposals": [], "rationale": "JSON parse failed"}
        except Exception:
            if attempt < LLM_MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                logger.exception(
                    "Holistic LLM call failed after %d retries (prompt len: %d)",
                    LLM_MAX_RETRIES, len(prompt),
                )
                return {"instruction_text": "", "example_sql_proposals": [], "rationale": "LLM call failed"}
    return {"instruction_text": "", "example_sql_proposals": [], "rationale": "All retries exhausted"}


# Canonical L5a section headers. MUST stay in sync with the
# <instructions> block in lever-5a-instructions/SKILL.md and the
# LEVER_5A_CANONICAL_SECTIONS tuple in
# tests/unit/optimization/test_lever_5a_instructions_prompt.py.
_LEVER_5A_SECTION_HEADERS: tuple[str, ...] = (
    "PURPOSE:", "ASSET ROUTING:", "BUSINESS DEFINITIONS:",
    "DISAMBIGUATION:", "AGGREGATION RULES:", "FUNCTION ROUTING:",
    "JOIN GUIDANCE:", "QUERY RULES:", "QUERY PATTERNS:",
    "TEMPORAL FILTERS:", "DATA QUALITY NOTES:", "CONSTRAINTS:",
)


def _compute_max_section_chars_for_l5a(instruction_text: str) -> int:
    """Return the size (in chars) of the longest canonical section
    in an L5a instruction document.

    Used by the max_section_chars MLflow tag (Task 9 of
    2026-05-17-lever-5a-instructions-hardening.md) to surface per-
    section density without re-deploying. Empty / un-sectioned input
    returns 0.
    """
    if not isinstance(instruction_text, str) or not instruction_text:
        return 0

    header_positions: list[tuple[str, int]] = []
    for header in _LEVER_5A_SECTION_HEADERS:
        idx = instruction_text.find(header)
        if idx >= 0:
            header_positions.append((header, idx))
    if not header_positions:
        return 0
    header_positions.sort(key=lambda t: t[1])

    max_chars = 0
    for i, (_, start) in enumerate(header_positions):
        end = (
            header_positions[i + 1][1]
            if i + 1 < len(header_positions)
            else len(instruction_text)
        )
        max_chars = max(max_chars, end - start)
    return max_chars


def _call_llm_for_lever_5a_instructions(
    all_clusters: list[dict],
    metadata_snapshot: dict,
    lever_changes: list[dict] | None = None,
    w: WorkspaceClient | None = None,
    *,
    raw_evidence: tuple[dict, ...] = (),
) -> dict:
    """Plan 2 — Lever 5a (instruction-only) LLM call.

    Sibling of ``_call_llm_for_holistic_instructions`` that uses
    ``LEVER_5A_INSTRUCTION_PROMPT`` and enforces the no-SQL output
    contract. Returns ``{"instruction_text": str, "rationale": str}``
    — no ``example_sql_proposals`` key.

    Capture-sink hit is unconditional; the sink itself is no-op
    unless ``GSO_LEVER5_SPLIT_CAPTURE_PATH`` is set (separate env var,
    not retired by the 2026-05-16 dead-flag cleanup).
    """
    from genie_space_optimizer.common.config import (
        LEVER_5A_INSTRUCTION_PROMPT,
    )
    from genie_space_optimizer.optimization.applier import _get_general_instructions

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    _mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])
    _funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])

    config = metadata_snapshot.get("config") or {}
    space_desc = config.get("description") or ""
    if isinstance(space_desc, list):
        space_desc = "\n".join(space_desc)
    if not space_desc:
        space_desc = "(No description set for this Genie Space.)"

    current_instructions = _get_general_instructions(metadata_snapshot)
    existing_example_sqls = _format_existing_example_sqls(metadata_snapshot)

    resolved_ids: set[str] = set()
    for lc in (lever_changes or []):
        for cid in (lc.get("cluster_ids", []) or []):
            if lc.get("status") in ("applied", "success"):
                resolved_ids.add(str(cid))

    unresolved = [
        c for c in all_clusters
        if str(c.get("cluster_id", "")) not in resolved_ids
    ]
    focus_clusters = unresolved if unresolved else all_clusters

    _allowlist = _build_identifier_allowlist(metadata_snapshot)

    # 5a budget is 1500 chars smaller than today's holistic budget so
    # the per-cluster L5b calls in Plan 4 have room for raw evidence.
    LEVER_5A_INSTRUCTION_BUDGET = max(0, 24500 - 500 - 1500)

    format_kwargs: dict[str, Any] = {
        "space_description": space_desc,
        "eval_summary": _format_eval_summary(focus_clusters),
        "cluster_briefs": _format_cluster_briefs_afs(focus_clusters, top_n=5),
        "lever_summary": _format_lever_summary(lever_changes),
        "current_instructions": current_instructions or "(No current instructions.)",
        "existing_example_sqls": existing_example_sqls,
        "instruction_char_budget": LEVER_5A_INSTRUCTION_BUDGET,  # INPUT budget (deprecated 2026-05-17 per Task 9; kept for back-compat)
        "instruction_output_char_budget": MAX_HOLISTIC_INSTRUCTION_CHARS,  # OUTPUT budget (Task 9)
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
        "raw_evidence_block": _format_raw_evidence_block(raw_evidence),
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, LEVER_5A_INSTRUCTION_PROMPT,
        priority_keys=["existing_example_sqls", "lever_summary",
                       "cluster_briefs", "eval_summary"],
    )

    prompt = format_mlflow_template(LEVER_5A_INSTRUCTION_PROMPT, **format_kwargs)

    from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
    _link_prompt_to_trace("lever_5a_instructions")

    # Capture-sink hit BEFORE the LLM call so a crash mid-call still
    # records that L5a was attempted. The sink is no-op unless
    # GSO_LEVER5_SPLIT_CAPTURE_PATH is set, so the unconditional call
    # is free in production.
    from genie_space_optimizer.common.config import _record_lever5_skill_hit
    _record_lever5_skill_hit("lever-5a-instructions")

    logger.info(
        "\n┌─── LLM Call [LEVER_5A_INSTRUCTIONS] ─────────────────────────────────\n"
        "│ Clusters: %d  Lever changes: %d  Prompt length: %d chars\n"
        "└─────────────────────────────────────────────────────────────────────────",
        len(focus_clusters), len(lever_changes or []), len(prompt),
    )

    import time
    from genie_space_optimizer.optimization.evaluation import _extract_json

    from genie_space_optimizer.common.config import (
        LEVER_5A_INSTRUCTION_SYSTEM_MSG,
    )
    system_msg = LEVER_5A_INSTRUCTION_SYSTEM_MSG

    from genie_space_optimizer.optimization.prompt_io import (
        Lever5aInstructionsOutput,
    )
    from genie_space_optimizer.common.config import (
        LEVER_5A_INSTRUCTION_MAX_TOKENS,
    )

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name="lever_5a_instructions",
            response_model=Lever5aInstructionsOutput,
            max_tokens=LEVER_5A_INSTRUCTION_MAX_TOKENS,
        )
    except Exception:
        logger.exception(
            "L5a LLM call failed after retries (prompt len: %d)", len(prompt),
        )
        return {"instruction_text": "", "rationale": "LLM call failed"}

    _repair_used = False
    try:
        result = _extract_json(text)
    except json.JSONDecodeError:
        # Task 5 — L5a-specific repair. Replaces the prior
        # _repair_truncated_holistic_json call which looked for
        # example_sql_proposals (wrong shape for L5a, see
        # baseline §6.A2).
        result = _repair_truncated_l5a_json(text)
        _repair_used = True
    if result is None or not isinstance(result, dict):
        # _extract_json returns None (not raises) on truncated / malformed
        # JSON in non-strict mode — fall through to the L5a repair so the
        # truncation case is still observable via _repair_used.
        result = _repair_truncated_l5a_json(text or "")
        _repair_used = True

    instruction_text = result.get("instruction_text", "")
    _sanitize_made_changes = False
    if instruction_text:
        _pre_sanitize = instruction_text
        instruction_text = _sanitize_plaintext_instructions(instruction_text)
        _sanitize_made_changes = (instruction_text != _pre_sanitize)
    rationale = result.get("rationale", "")

    _post_call_truncated = False
    if instruction_text and len(instruction_text) > MAX_HOLISTIC_INSTRUCTION_CHARS:
        logger.warning(
            "L5a instruction text exceeds %d chars (%d), truncating",
            MAX_HOLISTIC_INSTRUCTION_CHARS, len(instruction_text),
        )
        instruction_text = instruction_text[:MAX_HOLISTIC_INSTRUCTION_CHARS]
        _post_call_truncated = True

    candidate = {
        "instruction_text": instruction_text,
        "rationale": rationale,
    }
    ok, reject_reason = _validate_lever_5a_no_sql_output(candidate)

    # Task 9.4b: compute per-section sizes from the post-sanitize,
    # post-truncate instruction_text so the max_section_chars tag
    # reflects what the space actually receives.
    _max_section_chars = _compute_max_section_chars_for_l5a(instruction_text)

    # Task 4 — observability tags. Mirrors preflight Task 13 / cluster-
    # driven Task 8. Stay in sync with LEVER_5A_OBSERVABILITY_TAG_KEYS
    # in common/config.py.
    import mlflow as _mlflow
    try:
        _mlflow.update_current_trace(tags={
            "validate_no_sql_result": (
                "pass" if ok else f"rejected:{(reject_reason or '')[:60]}"
            ),
            "post_call_truncated": "true" if _post_call_truncated else "false",
            "repair_used": "true" if _repair_used else "false",
            "sanitize_made_changes": (
                "true" if _sanitize_made_changes else "false"
            ),
            "system_msg_version": "v2-slim",  # bumped from "v1" by Task 6
            "clusters_truncated": str(max(0, len(focus_clusters) - 5)),
            "rca_contract_version": "v1",
            "max_section_chars": str(_max_section_chars),
        })
    except Exception:
        logger.debug("L5a observability tag emission failed", exc_info=True)

    if not ok:
        logger.warning(
            "GSO_LEVER5A_REJECTED_V1: L5a output rejected by no-SQL gate: %s",
            reject_reason,
        )
        return {"instruction_text": "", "rationale": f"5a rejected: {reject_reason}"}
    return candidate


def _dispatch_lever_5b_for_cluster(
    cluster: dict,
    metadata_snapshot: dict,
    w: WorkspaceClient | None,
    benchmark_corpus: Any,
    benchmarks: list[dict] | None = None,
    # ── Plan 5 — LLM-driven RepairIntent synthesizer short-circuit ──
    rca_evidence_typed: Any = None,
    llm_cluster: Any = None,
    ag_id: str | None = None,
    iteration: int | None = None,
    run_id: str | None = None,
) -> list[dict]:
    """Plan 2 — adapter that returns the proposed example SQL(s) for ONE
    cluster in holistic-compatible shape.

    Plan B (2026-05-16) — when
    ``rich_synthesis_primary_for_sql_shape_enabled()`` is true AND the
    cluster carries a SQL-shape failure label (per ``cluster_failure_keys``
    ∩ ``_SQL_SHAPE_ROOT_CAUSES``), route to
    ``run_cluster_driven_synthesis_for_single_cluster`` via
    ``_dispatch_rich_synthesis_for_l5b``. Declines append to
    ``_L5B_RICH_PATH_DECLINES``; the harness drains the ledger after
    Stage-2 and emits ``NO_STRUCTURAL_CANDIDATE`` records.

    Lean-path (flag off OR non-SQL-shape cluster) preserves the original
    contract:
      * ``synthesize_example_sqls(cluster, metadata_snapshot,
        benchmark_corpus, *, archetype=None, budget=None,
        existing_example_sql_count=0, w=None, ...)`` — see
        ``synthesis.py:875+``.
      * Returns ``dict | None`` — a single validated proposal, or
        ``None`` when no archetype matches / caps exhausted / all
        gate attempts fail. The adapter wraps single-dict in a list and
        returns ``[]`` for ``None``.
      * AFS is built INTERNALLY by ``synthesize_example_sqls`` via
        ``format_afs(cluster)``; the adapter does NOT pre-compute it.
      * ``benchmark_corpus`` (a ``BenchmarkCorpus`` instance built from
        the strategy's ``benchmarks`` list) is required by the L5b
        firewall for n-gram leakage checks. Pass ``None`` only in tests
        where ``synthesize_example_sqls`` is monkeypatched.

    ``benchmarks`` (the raw list, not the corpus) is consumed by the
    rich path; the lean path ignores it. Caller
    (``_dispatch_lever_5_split``) threads it from
    ``generate_proposals_from_strategy``'s same-named arg. Stage-2
    bundles do not currently carry raw benchmarks; ``_stage_2_l5b``
    passes ``benchmarks=None`` and the rich path runs with an empty
    leakage corpus (degrades gracefully).
    """
    # ── Plan 5 — LLM-driven RepairIntent synthesizer short-circuit ──
    from genie_space_optimizer.common.config import (
        plan5_lever_5b_llm_intent_enabled,
    )
    # Plan 10 Phase A2 — gate no longer requires ``rca_evidence_typed``.
    # Production evidence from runs ``59a173d3`` (airline) and
    # ``ab65fefe`` (7now) showed the deterministic RCA classifier
    # returning empty ``per_qid_evidence_typed`` for every failing
    # anchor, which silently closed this gate (Leak 1) and forced
    # fallback to the legacy archetype path. The LLM-direct lane is
    # designed to work from the cluster's failure signal alone
    # (``cluster_semantic_theme`` + ``primary_blame_set`` +
    # ``sql_contexts``), so an empty ``rca_evidence_typed`` is a valid
    # input — the synthesizer's prompt will simply render
    # ``per_qid_evidence: []`` and lean on the typed cluster fields.
    if (
        plan5_lever_5b_llm_intent_enabled()
        and llm_cluster is not None
        and ag_id
    ):
        from genie_space_optimizer.optimization.repair_intent_synthesizer import (
            synthesize_repair_intent_for_cluster,
        )
        from genie_space_optimizer.optimization.cross_lever_router import (
            route_to_per_lever_generator,
        )
        from genie_space_optimizer.optimization.repair_intent import (
            stamp_repair_intent_on_proposal,
        )
        from genie_space_optimizer.optimization.failure_cluster import (
            FailureCluster,
        )
        from genie_space_optimizer.optimization.plan9_activation_markers import (
            ActivationStatus,
            emit_plan5_activation,
        )
        _plan5_run_id = str(run_id or "")
        _plan5_cluster_id = str(cluster.get("cluster_id") or "")
        _plan5_iter = int(iteration or 0)
        _plan5_ag_id = str(ag_id)

        identifier_allowlist: set[str] = set(
            metadata_snapshot.get("schema_columns") or []
        )
        if not identifier_allowlist:
            for ev in rca_evidence_typed.values():
                identifier_allowlist.update(ev.blame_set)
        existing_questions = []
        for ex in (metadata_snapshot.get("instructions", {}) or {}).get(
            "example_question_sqls", []
        ) or []:
            q = (ex or {}).get("question")
            if isinstance(q, str) and q.strip():
                existing_questions.append(q.strip())
        existing_preview = "; ".join(
            f"({i+1}) '{q}'" for i, q in enumerate(existing_questions[:5])
        )

        proposal = synthesize_repair_intent_for_cluster(
            w=w,
            cluster=llm_cluster,
            rca_evidence_typed=rca_evidence_typed,
            identifier_allowlist=identifier_allowlist,
            ag_id=ag_id,
            iteration=int(iteration or 0),
            seq=1,
            existing_examples_preview=existing_preview,
            benchmarks=benchmarks,
        )
        if proposal is None:
            emit_plan5_activation(
                run_id=_plan5_run_id,
                iteration=_plan5_iter,
                ag_id=_plan5_ag_id,
                cluster_id=_plan5_cluster_id,
                status=ActivationStatus.PLAN5_INTENT_DECLINED,
                reason="synthesizer_returned_none",
            )
        if proposal is not None:
            _activation_status = ActivationStatus.PLAN5_INTENT_INVOKED
            _activation_reason = "synthesizer_returned_proposal"
            _activation_patch_type = proposal.patch_type.value
            _activation_intent_id = proposal.intent_id
            # Plan 9 Task 6 — materialize RepairProposal.patch_body
            # directly via to_proposal_dict().
            # Plan 9 Task 6.1 — finalize ADD_SQL_SNIPPET_* patches.
            from genie_space_optimizer.optimization.repair_intent import PatchType
            from genie_space_optimizer.optimization.sql_snippet_finalizer import (
                finalize_sql_snippet_proposal_dict,
            )
            _SQL_SNIPPET_TYPES = {
                PatchType.ADD_SQL_SNIPPET_MEASURE,
                PatchType.ADD_SQL_SNIPPET_FILTER,
                PatchType.ADD_SQL_SNIPPET_EXPRESSION,
            }
            proposal_dict: dict | None = None
            materialization_source = "plan9_direct"
            try:
                base_dict = proposal.to_proposal_dict()
                if proposal.patch_type in _SQL_SNIPPET_TYPES:
                    proposal_dict = finalize_sql_snippet_proposal_dict(
                        proposal,
                        base_dict,
                        cluster=cluster,
                        metadata_snapshot=metadata_snapshot,
                        w=w, spark=spark,
                        catalog=catalog, gold_schema=gold_schema,
                        warehouse_id=warehouse_id,
                    )
                    if proposal_dict is None:
                        raise RuntimeError(
                            "plan9.finalizer_declined "
                            "intent_id=" + proposal.intent_id
                        )
                else:
                    proposal_dict = base_dict
            except Exception as exc:
                logger.warning(
                    "plan9.l5b_direct_materialization_failed "
                    "intent_id=%s err=%s — falling back to per-lever "
                    "generator (safety net).",
                    proposal.intent_id, exc,
                )
                materialization_source = "plan9_legacy_fallback"
                proposal_dict = None
                _activation_status = (
                    ActivationStatus.PLAN5_INTENT_VALIDATOR_REJECTED
                )
                _activation_reason = (
                    f"to_proposal_dict_raised:{type(exc).__name__}"
                )

            routed = route_to_per_lever_generator(proposal)
            override_event = routed[1] if routed else None

            if proposal_dict is None:
                # Safety-net path: invoke the per-lever generator.
                if routed is None:
                    emit_plan5_activation(
                        run_id=_plan5_run_id,
                        iteration=_plan5_iter,
                        ag_id=_plan5_ag_id,
                        cluster_id=_plan5_cluster_id,
                        status=ActivationStatus.PLAN5_INTENT_VALIDATOR_REJECTED,
                        reason="cross_lever_router_returned_none",
                        patch_type=_activation_patch_type,
                        intent_id=_activation_intent_id,
                    )
                    return []
                generator = routed[0]
                proposal_dict = generator(proposal)
                if proposal_dict is None:
                    emit_plan5_activation(
                        run_id=_plan5_run_id,
                        iteration=_plan5_iter,
                        ag_id=_plan5_ag_id,
                        cluster_id=_plan5_cluster_id,
                        status=ActivationStatus.PLAN5_INTENT_VALIDATOR_REJECTED,
                        reason="safety_net_generator_returned_none",
                        patch_type=proposal.patch_type.value,
                        intent_id=proposal.intent_id,
                    )
                    return []

            fc = FailureCluster.from_legacy(cluster)
            intent = proposal.to_repair_intent(cluster=fc, ag_id=ag_id)
            stamp_repair_intent_on_proposal(proposal_dict, intent)
            _prov = proposal_dict.setdefault("provenance", {})
            if isinstance(_prov, dict):
                _prov["plan9_materialization_source"] = (
                    materialization_source
                )
            if override_event is not None:
                proposal_dict["cross_lever_override"] = (
                    override_event.to_dict()
                )
                logger.info(
                    "plan5.cross_lever_override intent_id=%s "
                    "from=%s to=%s",
                    override_event.intent_id,
                    override_event.from_lever,
                    override_event.to_lever,
                )
            if override_event is not None:
                _activation_status = ActivationStatus.PLAN5_INTENT_ROUTED
                _activation_reason = (
                    f"routed_from_l5b_to_{override_event.to_lever}"
                )
            else:
                _activation_status = ActivationStatus.PLAN5_INTENT_MATERIALIZED
                _activation_reason = (
                    "patch_body materialized via to_proposal_dict"
                )
            emit_plan5_activation(
                run_id=_plan5_run_id,
                iteration=_plan5_iter,
                ag_id=_plan5_ag_id,
                cluster_id=_plan5_cluster_id,
                status=_activation_status,
                reason=_activation_reason,
                patch_type=_activation_patch_type,
                intent_id=_activation_intent_id,
            )
            logger.info(
                "plan9.l5b_materialized intent_id=%s cluster_id=%s "
                "ag_id=%s patch_type=%s source=%s",
                proposal.intent_id,
                cluster.get("cluster_id"),
                ag_id,
                proposal.patch_type.value,
                materialization_source,
            )
            return [proposal_dict]
        # LLM declined OR routing failed → fall through to the existing
        # rich-path / lean-path branches below.

    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        should_route_l5b_to_rich_synthesizer,
        _dispatch_rich_synthesis_for_l5b,
    )

    if should_route_l5b_to_rich_synthesizer(cluster):
        return _dispatch_rich_synthesis_for_l5b(
            cluster=cluster,
            metadata_snapshot=metadata_snapshot,
            w=w,
            benchmarks=benchmarks,
        )

    from genie_space_optimizer.optimization import synthesis

    try:
        proposal = synthesis.synthesize_example_sqls(
            cluster=cluster,
            metadata_snapshot=metadata_snapshot,
            benchmark_corpus=benchmark_corpus,
            budget=synthesis.SynthesisBudget.new(),
            w=w,
        )
    except Exception:
        logger.exception(
            "Lever 5b per-cluster synthesis failed for cluster %s",
            cluster.get("cluster_id", "?"),
        )
        return []

    if proposal is None:
        return []

    # Capture-sink hit AFTER a successful return so failed/skipped
    # syntheses don't pad the counter. Sink is no-op unless
    # GSO_LEVER5_SPLIT_CAPTURE_PATH is set.
    from genie_space_optimizer.common.config import _record_lever5_skill_hit
    _record_lever5_skill_hit("lever-5b-example-sql")

    proposal_dict = {
        "example_question": proposal.get("example_question", ""),
        "example_sql": proposal.get("example_sql", ""),
        "parameters": proposal.get("parameters", []) or [],
        "usage_guidance": proposal.get("usage_guidance", "")
                          or proposal.get("rationale", ""),
        "patch_type": "add_example_sql",
    }
    # Plan 8 Task 7 — stamp the typed RepairIntent on the lean fallback
    # proposal so ProposalSlate.repair_intents_by_id is non-empty when
    # the LLM intent short-circuit declines.
    #
    # Plan 11: stamp_proposals_from_archetype is a pure post-hoc utility
    # that decorates a proposal dict with archetype metadata. It is
    # shared between the legacy path and the Plan 11 LLM-first path
    # (the latter still annotates its proposals for postmortem
    # provenance); plan11_llm_first_enabled() does not change behavior
    # at this call site.
    archetype_name = str(proposal.get("_archetype_name") or "")
    if archetype_name:
        try:
            from genie_space_optimizer.optimization.archetypes import ARCHETYPES
            from genie_space_optimizer.optimization.cluster_driven_synthesis import (
                stamp_proposals_from_archetype,
            )
            from genie_space_optimizer.optimization.failure_cluster import (
                FailureCluster,
            )
            arch = next(
                (a for a in ARCHETYPES if a.name == archetype_name), None,
            )
            if arch is not None:
                fc = FailureCluster.from_legacy(cluster)
                stamp_proposals_from_archetype(
                    proposals=[proposal_dict],
                    archetype=arch,
                    cluster=fc,
                    ag_id=str(ag_id or ""),
                )
        except Exception:
            logger.debug(
                "Plan 8 Task 7 — lean fallback stamp failed (non-fatal)",
                exc_info=True,
            )
    return [proposal_dict]


def _dispatch_lever_5_split(
    all_clusters: list[dict],
    metadata_snapshot: dict,
    lever_changes: list[dict] | None = None,
    w: WorkspaceClient | None = None,
    benchmarks: list[dict] | None = None,
    # Plan 8 Task 2 — thread Plan 5 intent-aware kwargs from caller.
    rca_evidence_typed: dict | None = None,
    llm_cluster_by_cluster_id: dict | None = None,
    ag_id: str | None = None,
    iteration: int = 0,
    run_id: str | None = None,
) -> dict:
    """Plan 2 — split-mode dispatcher for Lever 5.

    Returns the SAME output shape as
    ``_call_llm_for_holistic_instructions`` so the rest of
    ``generate_proposals_from_strategy`` is unaffected:
        {"instruction_text": str,
         "example_sql_proposals": list[dict],
         "rationale": str}

    Internal fan-out:
      * one ``_call_llm_for_lever_5a_instructions`` call (merged
        instruction document for the whole AG),
      * one ``_dispatch_lever_5b_for_cluster`` call per input cluster
        (caps in ``synthesis.SynthesisBudget`` enforce per-cluster /
        per-archetype limits inside synthesize_example_sqls).

    ``benchmarks`` is threaded from
    ``generate_proposals_from_strategy``'s arg of the same name so the
    L5b leakage firewall has the corpus it needs.

    Plan 2 is unconditionally on as of the 2026-05-16 dead-flag
    cleanup. Shadow-comparison emission was a fixture-regen aid
    driven by a now-deleted shadow rollout flag; the byte-stable
    ``tests/fixtures/lever5_split_v1/`` fixtures are pinned in CI
    so the emission path is no longer needed.
    """
    # Task 13: wrap the dispatcher in an MLflow span so the 5a + 5b
    # fan-out is visible as a single parent in traces. Without this,
    # the lever_5a_instructions and lever_5b_example_sql spans appear
    # as siblings under a generic optimization span with no dispatch
    # context (cluster counts, dispatched_5a/5b flags).
    import mlflow as _mlflow

    cluster_list = list(all_clusters or [])
    _dispatched_5a = True  # always fires (one merged call for the AG)
    _dispatched_5b = len(cluster_list) > 0  # one call per cluster

    with _mlflow.start_span(
        name="lever_5_dispatch",
        attributes={
            "cluster_count": len(cluster_list),
            "dispatched_5a": _dispatched_5a,
            "dispatched_5b": _dispatched_5b,
        },
    ):
        # Build the benchmark corpus once and reuse for every cluster.
        benchmark_corpus = None
        try:
            from genie_space_optimizer.optimization.leakage import BenchmarkCorpus
            benchmark_corpus = BenchmarkCorpus.from_benchmarks(benchmarks or [])
        except Exception:
            logger.warning(
                "L5 split: unable to construct BenchmarkCorpus from "
                "%d benchmarks; L5b firewall will run with empty corpus",
                len(benchmarks or []),
                exc_info=True,
            )

        five_a = _call_llm_for_lever_5a_instructions(
            all_clusters=all_clusters,
            metadata_snapshot=metadata_snapshot,
            lever_changes=lever_changes,
            w=w,
        )

        example_sql_proposals: list[dict] = []
        _rca = rca_evidence_typed or {}
        _lcm = llm_cluster_by_cluster_id or {}
        for cluster in cluster_list:
            _cid = str(cluster.get("cluster_id") or "")
            example_sql_proposals.extend(
                _dispatch_lever_5b_for_cluster(
                    cluster=cluster,
                    metadata_snapshot=metadata_snapshot,
                    w=w,
                    benchmark_corpus=benchmark_corpus,
                    benchmarks=benchmarks,
                    rca_evidence_typed=_rca,
                    llm_cluster=_lcm.get(_cid),
                    ag_id=ag_id,
                    iteration=int(iteration),
                    run_id=run_id,
                )
            )

        rationale = (
            f"L5a: {five_a.get('rationale', '') or '(none)'}. "
            f"L5b: {len(example_sql_proposals)} example SQLs across "
            f"{len(cluster_list)} cluster(s)."
        )
        return {
            "instruction_text": five_a.get("instruction_text", "") or "",
            "example_sql_proposals": example_sql_proposals,
            "rationale": rationale,
        }


def _select_lever_5_holistic_path(
    all_clusters: list[dict],
    metadata_snapshot: dict,
    lever_changes: list[dict] | None = None,
    w: WorkspaceClient | None = None,
    benchmarks: list[dict] | None = None,
    *,
    # Plan 8 Task 2 — intent-aware kwargs forwarded to the split dispatcher.
    rca_evidence_typed: dict | None = None,
    llm_cluster_by_cluster_id: dict | None = None,
    ag_id: str | None = None,
    iteration: int = 0,
    run_id: str | None = None,
) -> dict:
    """Plan 2 is unconditionally on as of 2026-05-16 — the L5 result
    always comes from the split dispatcher. The historical selector
    that arbitrated between the split path, the shadow-emission
    branch, and the legacy holistic path is gone; this thin wrapper
    is kept so the existing call sites in
    ``generate_metadata_proposals`` stay unchanged.

    Plan 8 Task 2 — forward intent-aware kwargs to ``_dispatch_lever_5_split``.
    """
    return _dispatch_lever_5_split(
        all_clusters=all_clusters,
        metadata_snapshot=metadata_snapshot,
        lever_changes=lever_changes,
        w=w,
        benchmarks=benchmarks,
        rca_evidence_typed=rca_evidence_typed,
        llm_cluster_by_cluster_id=llm_cluster_by_cluster_id,
        ag_id=ag_id,
        iteration=int(iteration),
        run_id=run_id,
    )


def _emit_lever5_shadow_comparison(
    ag_id: str,
    cluster_ids: list[str],
    old: dict,
    new: dict,
) -> None:
    """Plan 2 — emit one shadow-comparison record to the L5 capture sink.

    The 2026-05-16 dead-flag cleanup removed the live callers of this
    function (it was called from the deleted ``_run_lever5_shadow_emission``
    helper). The body stays callable so the comparison-math unit tests
    (Jaccard, set-overlap, structural-diff) keep their direct entry
    point; the underlying ``_record_lever5_shadow_comparison`` sink
    no-ops unless ``GSO_LEVER5_SPLIT_CAPTURE_PATH`` is set."""
    from genie_space_optimizer.common.config import (
        _record_lever5_shadow_comparison,
    )

    import hashlib

    def _hash(s: str) -> str:
        return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:16]

    def _sql_set(props: list[dict] | None) -> set[str]:
        return {(p.get("example_sql") or "").strip()
                for p in (props or []) if (p.get("example_sql") or "").strip()}

    def _jaccard(a: str, b: str) -> float:
        ta = set((a or "").lower().split())
        tb = set((b or "").lower().split())
        if not ta and not tb:
            return 1.0
        union = ta | tb
        if not union:
            return 1.0
        return len(ta & tb) / len(union)

    def _overlap(a: set[str], b: set[str]) -> float:
        if not a and not b:
            return 1.0
        union = a | b
        if not union:
            return 1.0
        return len(a & b) / len(union)

    old_sql = _sql_set(old.get("example_sql_proposals"))
    new_sql = _sql_set(new.get("example_sql_proposals"))
    record = {
        "ag_id": ag_id,
        "cluster_ids": cluster_ids,
        "old_instruction_text_hash": _hash(old.get("instruction_text", "")),
        "new_5a_instruction_text_hash": _hash(new.get("instruction_text", "")),
        "instruction_text_jaccard": _jaccard(
            old.get("instruction_text", ""), new.get("instruction_text", ""),
        ),
        "old_example_sqls_count": len(old_sql),
        "new_example_sqls_count": len(new_sql),
        "example_sqls_set_overlap": _overlap(old_sql, new_sql),
        "old_example_sqls_hashes": sorted(_hash(s) for s in old_sql),
        "new_example_sqls_hashes": sorted(_hash(s) for s in new_sql),
    }
    _record_lever5_shadow_comparison(record)


# ═══════════════════════════════════════════════════════════════════════
# Phase 1 — Holistic Strategist
# ═══════════════════════════════════════════════════════════════════════

_EMPTY_STRATEGY: dict[str, Any] = {
    "action_groups": [],
    "global_instruction_rewrite": "",
    "rationale": "",
}


def _format_soft_signal_summary(soft_clusters: list[dict]) -> str:
    """Compact summary of soft-signal clusters for the strategist prompt."""
    if not soft_clusters:
        return "(No soft signals.)"
    _info_judges = {j for j, t in DEFAULT_THRESHOLDS.items() if t == 0.0}
    filtered: list[dict] = []
    for sc in soft_clusters:
        judges_in_cluster = {
            fj.get("judge", "")
            for qt in sc.get("question_traces", [])
            for fj in qt.get("failed_judges", [])
        }
        if judges_in_cluster and judges_in_cluster <= _info_judges:
            continue
        filtered.append(sc)
    if not filtered:
        return "(No actionable soft signals.)"
    lines: list[str] = []
    for sc in filtered[:10]:
        cid = sc.get("cluster_id", "?")
        rc = sc.get("root_cause", "unknown")
        qids = sc.get("question_ids", [])
        lines.append(f"- {cid}: root_cause={rc}, questions={len(qids)}")
        for qt in sc.get("question_traces", [])[:2]:
            qtext = qt.get("question_text", "")[:120]
            lines.append(f"    Q: {qtext}")
            for fj in qt.get("failed_judges", []):
                lines.append(
                    f"    Judge {fj.get('judge','?')}: {fj.get('resolved_root_cause','?')} "
                    f"— {fj.get('rationale_snippet','')[:150]}"
                )
    return "\n".join(lines) if lines else "(No soft signals.)"


def _format_join_specs_context(metadata_snapshot: dict) -> str:
    """Format current join specs for the strategist prompt.

    PR 29 — Specs whose left or right identifier resolves to a metric
    view in ``_asset_semantics`` are flagged inline as MV-incompatible
    so the strategist never proposes synthesis SQL that joins them
    directly.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    inst = metadata_snapshot.get("instructions", {})
    if not isinstance(inst, dict):
        inst = {}
    specs = (
        metadata_snapshot.get("join_specs", [])
        or inst.get("join_specs", [])
        or ds.get("join_specs", [])
    )
    if not specs:
        return "(No join specifications configured.)"
    lines: list[str] = []
    for js in specs:
        left = js.get("left", {})
        right = js.get("right", {})
        sql = js.get("sql", "")
        left_id = left.get("identifier", "?") if isinstance(left, dict) else "?"
        right_id = right.get("identifier", "?") if isinstance(right, dict) else "?"
        l_mv = _semantics_is_metric_view_id(metadata_snapshot, left_id)
        r_mv = _semantics_is_metric_view_id(metadata_snapshot, right_id)
        suffix = ""
        if l_mv or r_mv:
            mv_sides = []
            if l_mv:
                mv_sides.append("left")
            if r_mv:
                mv_sides.append("right")
            suffix = (
                f"  [SKIP: METRIC_VIEW on {'+'.join(mv_sides)}; "
                "use CTE-first pattern instead of direct JOIN]"
            )
        lines.append(
            f"- {left_id} <-> {right_id}: {str(sql)[:200]}{suffix}"
        )
    return "\n".join(lines)


def _repair_truncated_strategy_json(text: str) -> dict:
    """Extract action_groups from a truncated strategy JSON response.

    Attempts bracket-matching on the ``action_groups`` array first, then
    falls back to extracting ``global_instruction_rewrite`` if present.
    """
    result: dict[str, Any] = {**_EMPTY_STRATEGY, "rationale": "Recovered from truncated JSON"}

    m_ag = re.search(r'"action_groups"\s*:\s*\[', text)
    if m_ag:
        bracket_start = m_ag.end() - 1
        depth = 0
        for i in range(bracket_start, len(text)):
            if text[i] == "[":
                depth += 1
            elif text[i] == "]":
                depth -= 1
            if depth == 0:
                try:
                    result["action_groups"] = json.loads(text[bracket_start : i + 1])
                except json.JSONDecodeError:
                    pass
                break

    m_gi = re.search(r'"global_instruction_rewrite"\s*:\s*"', text)
    if m_gi:
        start = m_gi.end()
        j = start
        while j < len(text):
            ch = text[j]
            if ch == "\\" and j + 1 < len(text):
                j += 2
                continue
            if ch == '"':
                break
            j += 1
        else:
            j = len(text)
        raw = text[start:j]
        try:
            result["global_instruction_rewrite"] = json.loads(f'"{raw}"')
        except (json.JSONDecodeError, ValueError):
            result["global_instruction_rewrite"] = raw.replace('\\"', '"').replace("\\n", "\n")

    if not result["action_groups"] and not result["global_instruction_rewrite"]:
        raise json.JSONDecodeError("Could not extract strategy fields", text, 0)

    logger.warning(
        "Repaired truncated strategy JSON — %d action groups, %d chars instruction",
        len(result["action_groups"]),
        len(result.get("global_instruction_rewrite", "")),
    )
    return result


def _normalize_instruction_rewrite(raw: Any) -> dict | str:
    """Normalize ``global_instruction_rewrite`` from an LLM response.

    The prompt schema asks for a JSON object (``{section: text}``), but
    the LLM may return a plain string, a list, or ``None``.  This helper
    ensures downstream code always receives either a validated ``dict``
    or a sanitized ``str``.

    When the LLM returns a string that contains recognizable section
    headers (e.g. ``QUERY RULES: - bullet ...``), the function attempts
    to parse it into the preferred dict form so that downstream code
    follows the safer section-level merge path.
    """
    if isinstance(raw, dict):
        valid_keys = set(INSTRUCTION_SECTION_ORDER)
        return {k: str(v) for k, v in raw.items() if k in valid_keys and v is not None}
    if isinstance(raw, list):
        raw = "\n".join(str(item) for item in raw)
    if isinstance(raw, str) and raw.strip():
        sanitized = _sanitize_plaintext_instructions(raw)
        if len(sanitized) > MAX_HOLISTIC_INSTRUCTION_CHARS:
            sanitized = sanitized[:MAX_HOLISTIC_INSTRUCTION_CHARS]
        parsed = _try_parse_string_as_section_dict(sanitized)
        if parsed is not None:
            return parsed
        return sanitized
    return ""


def _try_parse_string_as_section_dict(text: str) -> dict[str, str] | None:
    """Attempt to parse a plain-text string into a ``{section: content}`` dict.

    First tries ``_parse_sections`` (which requires headers on their own line).
    If that fails, applies ``_INLINE_SECTION_RE`` to reformat inline
    ``"SECTION: content"`` into newline-separated form and retries.

    Returns ``None`` if no known section headers are found.
    """
    sections, preamble = _parse_sections(text)
    if not sections:
        reformatted = _INLINE_SECTION_RE.sub(r'\n\1:\n', text).strip()
        if reformatted != text:
            sections, preamble = _parse_sections(reformatted)

    if not sections:
        return None

    valid_keys = set(INSTRUCTION_SECTION_ORDER)
    result: dict[str, str] = {}
    for k, lines in sections.items():
        if k in valid_keys:
            result[k] = "\n".join(lines)
    if preamble:
        non_blank = [ln for ln in preamble if ln.strip()]
        if non_blank:
            existing = result.get("CONSTRAINTS", "")
            result["CONSTRAINTS"] = (
                (existing + "\n" if existing else "") + "\n".join(non_blank)
            )
    if result:
        logger.info(
            "Coerced string instruction_rewrite to dict with %d section(s): %s",
            len(result), sorted(result.keys()),
        )
        return result
    return None


def _truncate_on_boundary(text: str, max_len: int, ellipsis: str = "...") -> str:
    """T3.16: Truncate ``text`` to at most ``max_len`` characters, preferring
    a word boundary (whitespace or punctuation) over a mid-word cut.

    If ``len(text) <= max_len`` the original string is returned unchanged
    and no ellipsis is appended. Otherwise the function looks backwards
    from ``max_len`` for the last run of whitespace or common sentence
    punctuation (``.,;:!?)]}``) and cuts there — falling back to a hard
    slice if no reasonable boundary exists within the last ~20% of the
    window.
    """
    if text is None:
        return ""
    s = str(text)
    if max_len <= 0:
        return ""
    if len(s) <= max_len:
        return s
    candidate = s[: max_len]
    _lookback = max(1, max_len // 5)
    _min_boundary = max_len - _lookback
    boundary_idx = -1
    for i in range(max_len - 1, _min_boundary - 1, -1):
        ch = candidate[i]
        if ch.isspace() or ch in ".,;:!?)]}":
            boundary_idx = i + 1
            break
    if boundary_idx <= 0:
        boundary_idx = max_len
    return candidate[:boundary_idx].rstrip() + ellipsis


def _preview_instruction_rewrite(rewrite: dict | str, max_chars: int = 200) -> str:
    """Human-readable preview of an instruction rewrite for logging.

    T3.16: uses ``_truncate_on_boundary`` so previews no longer slice
    through mid-word — prevents log lines like
    ``ASSET ROUTING: use fact_sales whenev...``.
    """
    if isinstance(rewrite, dict):
        parts = [
            f"{k}: {_truncate_on_boundary(str(v), 60)}" if len(str(v)) > 60 else f"{k}: {v}"
            for k, v in rewrite.items() if v
        ]
        preview = "; ".join(parts)
        return _truncate_on_boundary(preview, max_chars, ellipsis="...")
    return _truncate_on_boundary(str(rewrite), max_chars, ellipsis="...")


def _call_llm_for_strategy(
    clusters: list[dict],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict:
    """Monolithic strategist fallback — used only when triage returns 0 AGs.

    Sends the full STRATEGIST_PROMPT with compressed context (top-5 clusters,
    SQL truncated to 300 chars) to stay within timeout bounds.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions
    from genie_space_optimizer.optimization.evaluation import (
        _extract_json,
        _link_prompt_to_trace,
    )

    _blame_items: list[str] = []
    for c in clusters:
        _blame_items.extend(_normalize_blame(c.get("asi_blame_set")))
    blame_set: list[str] | None = list(dict.fromkeys(_blame_items)) if _blame_items else None

    format_kwargs: dict[str, Any] = {
        "full_schema_context": _format_full_schema_context(metadata_snapshot),
        "cluster_briefs": _format_cluster_briefs_afs(clusters, top_n=5),
        "soft_signal_summary": _format_soft_signal_summary(soft_signal_clusters),
        "structured_table_context": _format_structured_table_context(
            metadata_snapshot, blame_set, lever=1,
        ),
        "structured_column_context": _format_structured_column_context(
            metadata_snapshot, blame_set, lever=1,
        ),
        "structured_function_context": _format_structured_function_context(
            metadata_snapshot, lever=3,
        ),
        "current_join_specs": _format_join_specs_context(metadata_snapshot),
        "current_instructions": (
            _get_general_instructions(metadata_snapshot) or "(No current instructions.)"
        ),
        "existing_example_sqls": _format_existing_example_sqls(metadata_snapshot),
        "blamed_column_values": _format_blamed_column_values(
            clusters, metadata_snapshot.get("_data_profile", {}),
        ),
        "instruction_char_budget": max(0, 24500 - 500),
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, STRATEGIST_PROMPT,
        priority_keys=["blamed_column_values", "full_schema_context", "existing_example_sqls",
                        "soft_signal_summary", "structured_function_context",
                        "structured_column_context", "cluster_briefs"],
    )

    prompt = format_mlflow_template(STRATEGIST_PROMPT, **format_kwargs)

    _W = 78
    _hdr = "┌─── LLM Call [STRATEGIST] " + "─" * max(0, _W - 27)
    _ftr = "└" + "─" * (_W - 1)
    logger.info(
        "\n%s\n│ Hard clusters: %d\n│ Soft clusters: %d\n│ Prompt length: %d chars\n%s",
        _hdr, len(clusters), len(soft_signal_clusters), len(prompt), _ftr,
    )
    print(
        f"\n{'=' * _W}\n"
        f"  PHASE 1: HOLISTIC STRATEGIST\n"
        f"  Hard clusters: {len(clusters)} | Soft clusters: {len(soft_signal_clusters)}\n"
        f"  Prompt: {len(prompt):,} chars\n"
        f"{'=' * _W}"
    )

    _link_prompt_to_trace("strategist")

    system_msg = (
        "You are a JSON API. You MUST respond with ONLY a valid JSON object. "
        "Do NOT include any explanation, analysis, or markdown outside the JSON. "
        "Your entire response must be parseable by json.loads(). "
        "The JSON must contain an 'action_groups' array."
    )

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name="monolithic_strategy_fallback",
        )
    except Exception:
        logger.exception(
            "Strategist LLM call failed after retries (prompt len: %d)", len(prompt),
        )
        return {**_EMPTY_STRATEGY, "rationale": "LLM call failed"}

    try:
        result = _extract_json(text)
    except json.JSONDecodeError:
        try:
            result = _repair_truncated_strategy_json(text)
        except json.JSONDecodeError:
            logger.warning("Strategist LLM response was not valid JSON: %.500s", text)
            return {**_EMPTY_STRATEGY, "rationale": "JSON parse failed"}

    action_groups = result.get("action_groups", [])
    if not isinstance(action_groups, list):
        action_groups = []
    global_rewrite = _normalize_instruction_rewrite(result.get("global_instruction_rewrite"))
    rationale = result.get("rationale", "")

    _rewrite_preview = _preview_instruction_rewrite(global_rewrite)
    _rewrite_desc = (
        f"{len(global_rewrite)} sections" if isinstance(global_rewrite, dict)
        else f"{len(global_rewrite)} chars"
    )
    logger.info(
        "\n┌─── LLM Response [STRATEGIST] ────────────────────────────────────────\n"
        "│ Action groups: %d\n"
        "│ Global instruction rewrite: %s\n"
        "│ Rationale: %s\n"
        "└─────────────────────────────────────────────────────────────────────────",
        len(action_groups), _rewrite_desc, _truncate_on_boundary(str(rationale), 300),
    )
    print(
        f"\n  Strategy produced {len(action_groups)} action group(s), "
        f"{_rewrite_desc} instruction rewrite"
    )
    for i, ag in enumerate(action_groups):
        levers = sorted(ag.get("lever_directives", {}).keys())
        qs = ag.get("affected_questions", [])
        print(
            f"    AG{i + 1}: {ag.get('root_cause_summary', '?')[:80]}"
            f" | levers={levers} | questions={len(qs)}"
        )

    return {
        "action_groups": action_groups,
        "global_instruction_rewrite": global_rewrite,
        "rationale": rationale,
    }


# ── Adaptive Strategist (single-call, one AG per iteration) ─────────────


def _emit_strategist_context_records_for_test_harness(
    *,
    clusters: list[dict],
    reflection_buffer: list[dict],
    metadata_snapshot: dict,
    run_id: str,
    iteration: int,
    decision_emit: Any = None,
    mlflow_anchor_run_id: str | None = None,
) -> dict:
    """Plan P-G helper: compute Stage 4 typed boundary, emit
    STRATEGIST_CONTEXT_ASSEMBLED if both gating flags are on, persist
    the typed output JSON via MLflow log_text when an anchor is
    supplied. Returns a dict ``{"assembled_hash": str,
    "top_level_fields": tuple[str, ...]}`` so the caller can pass both
    structural-diff inputs to the CONSUMED emit at the LLM-call
    boundary. When the flags are off (or any internal step fails),
    returns the no-op shape ``{"assembled_hash": "",
    "top_level_fields": ()}`` so the caller does not need to branch on
    whether the helper ran.

    Public-but-underscored so the unit tests can exercise the helper
    without spinning up the full strategist call. Naming mirrors
    existing test-harness conventions in this module.
    """
    _NOOP: dict = {"assembled_hash": "", "top_level_fields": ()}
    from genie_space_optimizer.common.config import (
        stage_handlers_chunk_a_enabled,
        stage4_context_persistence_enabled,
    )
    if not stage_handlers_chunk_a_enabled():
        return _NOOP
    if not stage4_context_persistence_enabled():
        return _NOOP
    from genie_space_optimizer.optimization.stages import (
        strategist_context as _sc_stage,
    )
    _sc_clusters_by_qid: dict[str, str] = {}
    _sc_rca_cards: list[dict] = []
    for _c in (clusters or []):
        _cid = str(_c.get("cluster_id") or "")
        for _q in (_c.get("question_ids") or []):
            _sc_clusters_by_qid[str(_q)] = _cid
        if _c.get("rca_id") or _c.get("grounding"):
            _sc_rca_cards.append({
                "rca_id": str(_c.get("rca_id") or ""),
                "cluster_id": _cid,
                "grounding": str(_c.get("grounding") or "grounded"),
                "evidence_qids": list(_c.get("question_ids") or []),
            })
    _hard_qids = tuple(
        str(q) for c in (clusters or [])
        for q in (c.get("question_ids") or [])
    )
    _sc_inp = _sc_stage.StrategistContextInput(
        hard_failure_qids=_hard_qids,
        clusters_by_qid=_sc_clusters_by_qid,
        rca_cards=tuple(_sc_rca_cards),
        reflection_buffer=tuple(reflection_buffer or []),
        baseline_accuracy=float(
            metadata_snapshot.get("_baseline_accuracy") or 0.0
        ),
        iteration=int(iteration or 0),
    )
    _sc_out = _sc_stage.execute(ctx=None, inp=_sc_inp)
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_assembled_record,
    )
    record = strategist_context_assembled_record(
        run_id=run_id, iteration=iteration, assembled_output=_sc_out,
    )
    if decision_emit is not None:
        try:
            decision_emit(record)
        except Exception:
            logger.debug(
                "Plan P-G: decision_emit raised (non-fatal)",
                exc_info=True,
            )
    if mlflow_anchor_run_id:
        try:
            import json as _json_pg
            from genie_space_optimizer.optimization.run_output_contract import (
                stage_artifact_paths,
            )
            from genie_space_optimizer.optimization import (
                stage_io_capture as _sio,
            )
            paths = stage_artifact_paths(
                int(iteration or 0), "strategist_context",
            )
            _sio._log_text(
                run_id=str(mlflow_anchor_run_id),
                text=_json_pg.dumps(
                    _sc_out.to_json(),
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
                artifact_file=paths["output"],
            )
        except Exception:
            logger.debug(
                "Plan P-G: typed-output persistence failed (non-fatal)",
                exc_info=True,
            )
    return {
        "assembled_hash": str(record.metrics.get("assembled_hash") or ""),
        "top_level_fields": tuple(
            record.metrics.get("top_level_fields") or ()
        ),
    }


def _emit_strategist_context_consumed_for_test_harness(
    *,
    consumed_payload: Any,
    assembled_hash: str,
    assembled_top_fields: tuple = (),
    run_id: str,
    iteration: int,
    decision_emit: Any = None,
    mlflow_anchor_run_id: str | None = None,
) -> None:
    """Plan P-G — emit STRATEGIST_CONTEXT_CONSUMED at the strategist
    LLM-call boundary AND persist the consumed payload as
    ``consumed.json`` under the Stage 4 artifact directory (co-located
    with ``output.json`` from Task 7) so the two files can be diffed
    directly to verify drift.

    ``consumed_payload`` is the dict that becomes the LLM prompt's
    context_json. ``assembled_hash`` and ``assembled_top_fields`` are
    the values stamped on the matching ASSEMBLED record (empty when
    Stage 4 emit was skipped).

    Gated on GSO_STAGE4_CONTEXT_PERSISTENCE only — when off, the
    function is a no-op so replay byte-stability is preserved. Unlike
    the ASSEMBLED helper, this one runs even when
    GSO_STAGE_HANDLERS_CHUNK_A is off, because the consumed boundary
    is computable from the legacy kwargs-soup alone; the empty
    assembled_hash branch in the producer reports
    CONTEXT_CONSUMED_MATCHES_ASSEMBLED in that case.
    """
    from genie_space_optimizer.common.config import (
        stage4_context_persistence_enabled,
    )
    if not stage4_context_persistence_enabled():
        return
    if decision_emit is None:
        return
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_consumed_record,
    )
    try:
        record = strategist_context_consumed_record(
            run_id=str(run_id or ""),
            iteration=int(iteration or 0),
            consumed_payload=consumed_payload,
            assembled_hash=str(assembled_hash or ""),
            assembled_top_level_fields=tuple(assembled_top_fields or ()),
        )
        decision_emit(record)
    except Exception:
        logger.debug(
            "Plan P-G: CONSUMED emit failed (non-fatal)",
            exc_info=True,
        )
    # Persist the consumed payload under the Stage 4 artifact dir so a
    # postmortem reader can diff stages/04_strategist_context/
    # output.json vs consumed.json directly. The artifact lives under
    # the Stage 4 directory (not Stage 5) because the two halves of
    # the drift comparison belong together; placing them in different
    # directories would force every diff tool to know the cross-stage
    # mapping.
    if mlflow_anchor_run_id:
        try:
            import json as _json_pg_consumed
            from genie_space_optimizer.optimization.run_output_contract import (
                stage_artifact_paths,
            )
            from genie_space_optimizer.optimization import (
                stage_io_capture as _sio_consumed,
            )
            paths = stage_artifact_paths(
                int(iteration or 0), "strategist_context",
            )
            # paths["output"] is "<...>/04_strategist_context/output.json";
            # swap the filename to land consumed.json alongside it.
            consumed_path = paths["output"].rsplit("/", 1)[0] + "/consumed.json"
            _sio_consumed._log_text(
                run_id=str(mlflow_anchor_run_id),
                text=_json_pg_consumed.dumps(
                    consumed_payload, sort_keys=True,
                    separators=(",", ":"), default=str,
                ),
                artifact_file=consumed_path,
            )
        except Exception:
            logger.debug(
                "Plan P-G: consumed.json persistence failed (non-fatal)",
                exc_info=True,
            )


def _call_llm_for_adaptive_strategy(
    clusters: list[dict],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    reflection_buffer: list[dict],
    priority_ranking: list[dict],
    tried_patches: set[tuple[str, str]],
    w: WorkspaceClient | None = None,
    *,
    total_benchmarks: int = 0,
    passing_benchmarks: int = 0,
    verdict_history: dict | None = None,
    skill_exemplars: list[dict] | None = None,
    human_suggestions: list[dict] | None = None,
    iq_scan_summary: dict | None = None,
    max_ag_patches: int | None = None,
    intent_collisions: list[dict] | None = None,
    prior_iteration_dropped_causal_patches: list | tuple | None = None,
    # Plan P-G: Stage 4 boundary observability. All four kwargs default
    # to "no-op" so existing callers (replay fixtures, legacy paths)
    # remain byte-stable.
    run_id: str = "",
    iteration: int = 0,
    decision_emit: "Callable[[Any], None] | None" = None,
    mlflow_anchor_run_id: str | None = None,
    # Phase 3 T3.1.6: prior-iteration feedback packet built by
    # ``build_iteration_feedback``. ``None`` means "no prior iteration"
    # or "iteration feedback flag disabled" — both legacy-safe.
    iteration_feedback: "Any | None" = None,
) -> dict:
    """Single-call strategist that produces exactly ONE action group.

    Combines schema context, failure clusters, SQL diffs, a priority
    ranking, and a reflection buffer into one prompt.  Designed for the
    adaptive lever loop where this call is made every iteration with
    fresh evaluation results.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions
    from genie_space_optimizer.optimization.evaluation import (
        _extract_json,
        _link_prompt_to_trace,
    )

    # ── Cap budget visibility (v2 Task 5) ────────────────────────────
    # Surface MAX_AG_PATCHES to the strategist so it sizes ActionGroups
    # against the cap. Without this, multi-cluster bundles emit N×3+
    # patches and the cap drops most of them.
    from genie_space_optimizer.common.config import (
        MAX_AG_PATCHES as _CFG_MAX_AG_PATCHES,
    )
    _budget = int(max_ag_patches or _CFG_MAX_AG_PATCHES)
    budget_text = _format_strategist_budget_preamble(
        budget=_budget, n_clusters=len(clusters),
    )

    # v2 Task 12 — surface cross-cluster intent collisions to the
    # strategist prompt. When the LLM omits the corresponding
    # ``add_conditional_disambiguation_instruction`` patch, we emit a
    # deterministic one in the post-processing block below.
    intent_collision_text = ""
    if intent_collisions:
        intent_collision_text = (
            "INTENT COLLISIONS DETECTED — emit add_conditional_disambiguation_instruction "
            "patches for each collision below; do not pick a single global mapping:\n"
        )
        for c in intent_collisions:
            intent_collision_text += (
                f"  - term '{c['term']}' resolves to "
                f"{sorted(c['column_choices'])} across clusters "
                f"{sorted({cid for cids in c['clusters_by_column'].values() for cid in cids})}\n"
            )

    _blame_items: list[str] = []
    for c in clusters:
        _blame_items.extend(_normalize_blame(c.get("asi_blame_set")))
    blame_set: list[str] | None = list(dict.fromkeys(_blame_items)) if _blame_items else None

    # ── Build priority ranking text ──────────────────────────────────
    ranking_text = format_strategist_ranking_text(priority_ranking)

    # ── Build success summary ────────────────────────────────────────
    failing = total_benchmarks - passing_benchmarks
    success_summary = (
        f"{passing_benchmarks} of {total_benchmarks} benchmarks pass all judges. "
        f"{failing} failures remain."
        if total_benchmarks > 0
        else "(benchmark counts not available)"
    )

    # ── Build reflection text ────────────────────────────────────────
    reflection_text = format_reflection_buffer(reflection_buffer)

    # ── Build per-question persistence summary ────────────────────────
    from genie_space_optimizer.optimization.harness import (
        _build_question_persistence_summary,
    )
    persistence_text, _persistence_structured = _build_question_persistence_summary(
        verdict_history or {}, reflection_buffer,
    )

    # ── Build proven patterns text ────────────────────────────────────
    _proven_lines: list[str] = []
    for _ex in (skill_exemplars or []):
        _proven_lines.append(
            f"- Root cause: {_ex.get('root_cause', '?')[:80]} | "
            f"Levers: {', '.join(str(l) for l in _ex.get('lever_pattern', []))} | "
            f"Patches: {', '.join(str(x) for x in _ex.get('patch_types', [])[:4] if x is not None)} | "
            f"Gain: {_ex.get('accuracy_gain', 0):+.1f}%"
        )
    proven_patterns_text = (
        "\n".join(_proven_lines)
        if _proven_lines
        else "(No accepted patterns yet. This is informed by prior successful iterations.)"
    )

    # ── Build human reviewer suggestions text ──────────────────────────
    suggestions_text = ""
    if human_suggestions:
        _TYPE_LABELS = {
            "column_description": "Column descriptions",
            "business_rule": "Business rules",
            "join_condition": "Join conditions",
            "instruction": "Instructions",
            "general": "General suggestions",
        }
        grouped: dict[str, list[str]] = {}
        for s in human_suggestions:
            ttype = s.get("target_type", "general")
            target = s.get("target_identifier", "")
            suggestion = s.get("suggestion", "")
            if not suggestion:
                for item in s.get("suggestions", []):
                    grouped.setdefault("general", []).append(str(item))
                continue
            label = f"{target}: {suggestion}" if target else suggestion
            grouped.setdefault(ttype, []).append(label)
        lines_hs: list[str] = ["Human reviewer suggestions from prior review:"]
        for ttype, type_label in _TYPE_LABELS.items():
            items = grouped.get(ttype)
            if not items:
                continue
            lines_hs.append(f"\n{type_label}:")
            for item in items:
                lines_hs.append(f"- {item}")
        suggestions_text = "\n".join(lines_hs)

    iq_scan_text = _format_iq_scan_findings(iq_scan_summary)
    rca_theme_context = ""
    if ENABLE_RCA_THEMES_STRATEGIST:
        rca_theme_context = _format_rca_themes_for_strategy(
            metadata_snapshot.get("_rca_themes") or [],
            metadata_snapshot.get("_rca_theme_conflicts") or [],
        )

    # ── Build structured context ────────────────────────────────────
    # C15 Phase 2 Task 2.7: when GSO_STAGE_HANDLERS_CHUNK_A is on,
    # emit a typed StrategistContextOutput that captures the exact
    # boundary the strategist sees — enforcing the Stage 2→4 arrow
    # (grounded-only RCA cards). The output is currently observability-
    # only: the prompt still uses context_data from _build_context_data.
    # Future phases will refactor the strategist prompt to consume
    # rca_cards_grounded_only from the typed output directly.
    # Default-off guarantees legacy byte-stability.
    # C15 Phase 2 Task 2.7 + Plan P-G (2026-05-12) — when
    # GSO_STAGE_HANDLERS_CHUNK_A is on, compute the typed
    # StrategistContextOutput. When GSO_STAGE4_CONTEXT_PERSISTENCE is
    # additionally on, route the output through the Plan P-G emit
    # helper so the assembled boundary is observable in the trace and
    # the typed JSON lands in
    # gso_postmortem_bundle/iterations/iter_NN/stages/04_strategist_context/output.json.
    # Default-off guarantees legacy byte-stability for replay fixtures.
    _assembled_hash: str = ""
    _assembled_top_fields: tuple[str, ...] = ()
    try:
        _sc_state = _emit_strategist_context_records_for_test_harness(
            clusters=clusters or [],
            reflection_buffer=reflection_buffer or [],
            metadata_snapshot=metadata_snapshot or {},
            run_id=str(run_id or ""),
            iteration=int(iteration or 0),
            decision_emit=decision_emit,
            mlflow_anchor_run_id=mlflow_anchor_run_id,
        )
        _assembled_hash = str(_sc_state.get("assembled_hash") or "")
        _assembled_top_fields = tuple(
            _sc_state.get("top_level_fields") or ()
        )
    except Exception:
        logger.debug(
            "Plan P-G: Stage 4 boundary emit failed (non-fatal)",
            exc_info=True,
        )

    context_data = _build_context_data(
        clusters=clusters,
        soft_signal_clusters=soft_signal_clusters,
        metadata_snapshot=metadata_snapshot,
        reflection_buffer=reflection_buffer,
        priority_ranking=priority_ranking,
        blame_set=blame_set,
        success_summary=success_summary,
        reflection_text=reflection_text,
        persistence_text=persistence_text,
        proven_patterns_text=proven_patterns_text,
        suggestions_text=suggestions_text,
        iq_scan_text=iq_scan_text,
        rca_theme_context=rca_theme_context,
    )
    context_data = _truncate_context_to_budget(context_data, _adaptive_context_budget_tokens())
    context_json = json.dumps(context_data, indent=2, default=str)

    # Plan P-G: hash the dict that becomes the LLM prompt's context_json,
    # emit STRATEGIST_CONTEXT_CONSUMED so drift against the matching
    # ASSEMBLED record is observable in the operator transcript, and
    # persist the consumed payload as consumed.json under the Stage 4
    # artifact dir so postmortem can diff the two co-located JSONs.
    _emit_strategist_context_consumed_for_test_harness(
        consumed_payload=context_data,
        assembled_hash=_assembled_hash,
        assembled_top_fields=_assembled_top_fields,
        run_id=str(run_id or ""),
        iteration=int(iteration or 0),
        decision_emit=decision_emit,
        mlflow_anchor_run_id=mlflow_anchor_run_id,
    )

    format_kwargs: dict[str, Any] = {
        "context_json": context_json,
        "identifier_allowlist": _format_identifier_allowlist(
            _build_identifier_allowlist(metadata_snapshot)
        ),
        "instruction_char_budget": max(0, 24500 - 500),
    }

    prompt = format_mlflow_template(ADAPTIVE_STRATEGIST_PROMPT, **format_kwargs)
    # v2 Task 5: prepend cap budget so the strategist sees it before any
    # cluster bundling instructions in the templated prompt body.
    # v2 Task 12: intent_collision_text follows the budget so collisions
    # are surfaced before the cluster narrative; it is empty when no
    # collisions were detected.
    # Cycle 5 T2 closeout: surface the prior iteration's dropped causal
    # patches BEFORE the cluster narrative so the strategist sees what
    # was rejected and can propose a narrower variant or shift levers.
    # Gated by GSO_CAUSAL_DROP_FEEDBACK_TO_STRATEGIST at the harness
    # caller; ``prior_iteration_dropped_causal_patches`` is None /
    # empty when the flag is off, so the rendered text is empty.
    _t2_dropped_text = format_prior_dropped_causal_patches_text(
        prior_iteration_dropped_causal_patches or ()
    )
    if _t2_dropped_text:
        prompt = (
            budget_text + "\n\n"
            + intent_collision_text + "\n"
            + _t2_dropped_text + "\n\n"
            + prompt
        )
    else:
        prompt = budget_text + "\n\n" + intent_collision_text + "\n" + prompt

    # Phase 3 T3.1.6 — render prior-iteration feedback as a block the
    # strategist can read BEFORE the cluster narrative. The packet is
    # built by ``build_iteration_feedback`` and includes the four-tier
    # acceptance class, target-qid resolution (fixed vs still-hard),
    # regression-debt classification, prior tried AG-shapes per target,
    # and any near-miss reflections from the prior iteration.
    if iteration_feedback is not None:
        try:
            _feedback_text = _format_iteration_feedback_block(iteration_feedback)
        except Exception:
            logger.debug(
                "Phase 3: iteration-feedback block render skipped (non-fatal)",
                exc_info=True,
            )
            _feedback_text = ""
        if _feedback_text:
            prompt = _feedback_text + "\n\n" + prompt

    _W = 78
    _iter_label = len(reflection_buffer) + 1
    logger.info(
        "\n┌─── LLM Call [ADAPTIVE STRATEGIST] %s\n"
        "│ Clusters: %d hard, %d soft\n"
        "│ Reflections: %d\n"
        "│ Prompt: %d chars\n"
        "└%s",
        "─" * max(0, _W - 37),
        len(clusters),
        len(soft_signal_clusters),
        len(reflection_buffer),
        len(prompt),
        "─" * (_W - 1),
    )
    _input_lines = [
        f"\n{'=' * _W}",
        f"  STRATEGIST INPUT (Iteration {_iter_label})",
        f"{'=' * _W}",
        f"|  {'Success Summary:':<28s} {success_summary}",
        f"|  {'Clusters:':<28s} {len(clusters)} hard, {len(soft_signal_clusters)} soft",
        f"|  {'Reflections:':<28s} {len(reflection_buffer)}",
        f"|  {'Prompt:':<28s} {len(prompt):,} chars",
    ]
    _ranking_preview = ranking_text.split("\n")[:5]
    if _ranking_preview:
        _input_lines.append(f"|  {'Priority Ranking (top 5):':<28s}")
        for _rp in _ranking_preview:
            _input_lines.append(f"|    {_rp.strip()}")
    _refl_preview = reflection_text[:500]
    if _refl_preview and _refl_preview != "(No prior iterations. This is the first attempt after baseline evaluation.)":
        _input_lines.append(f"|  {'Reflection Buffer:':<28s}")
        for _rl in _refl_preview.split("\n")[:10]:
            _input_lines.append(f"|    {_rl}")
        if len(reflection_text) > 500:
            _input_lines.append(f"|    ... ({len(reflection_text) - 500} more chars)")
    _persist_preview = persistence_text[:500]
    if _persist_preview and "No" not in _persist_preview[:30]:
        _input_lines.append(f"|  {'Persistence Summary:':<28s}")
        for _pl in _persist_preview.split("\n")[:8]:
            _input_lines.append(f"|    {_pl}")
    if _proven_lines:
        _input_lines.append(f"|  {'Proven Patterns:':<28s} {len(_proven_lines)}")
        for _pp in _proven_lines[:3]:
            _input_lines.append(f"|    {_pp}")
    _input_lines.append(f"{'=' * _W}")
    print("\n".join(_input_lines))

    _link_prompt_to_trace("adaptive_strategist")

    system_msg = (
        "You are a JSON API. You MUST respond with ONLY a valid JSON object. "
        "Do NOT include any explanation, analysis, or markdown outside the JSON. "
        "Your entire response must be parseable by json.loads(). "
        "The JSON must contain an 'action_groups' array with EXACTLY one entry. "
        # Tier 2.5: scope bound — one source cluster per AG.
        "The action group MUST target exactly ONE source cluster. Multiple "
        "failure signatures require multiple iterations, not one giant AG. "
        # T2.16: require an explicit primary_cluster_id plus optional
        # secondary_cluster_ids. source_cluster_ids is still accepted for
        # backward-compatible replay; validator promotes the first entry
        # to primary_cluster_id and rejects cross-namespace spans
        # (mixing H### with S###) unless secondary_cluster_ids was set.
        "Set 'primary_cluster_id' to the single cluster this AG addresses "
        "(H### for hard, S### for soft). If the AG legitimately spans "
        "both hard and soft clusters that share a blame set, add the "
        "cross-namespace IDs to 'secondary_cluster_ids'. For backward "
        "compatibility you may instead populate 'source_cluster_ids' "
        "with a list of length 1; the validator will migrate it to "
        "primary_cluster_id. If the same root cause spans multiple "
        "clusters with the same blame set, pick the highest impact one; "
        "the remaining clusters will be addressed in subsequent iterations."
    )

    try:
        from genie_space_optimizer.optimization.prompt_io import (
            AdaptiveStrategistOutput,
        )
        text, _response = _traced_llm_call(
            w,
            system_msg,
            prompt,
            span_name="adaptive_strategy",
            response_validator=_adaptive_strategist_response_validator,
            response_model=AdaptiveStrategistOutput,
        )
    except Exception:
        logger.exception("Adaptive strategist LLM call failed after retries")
        return {**_EMPTY_STRATEGY, "rationale": "LLM call failed"}

    try:
        result = _extract_json(text)
    except json.JSONDecodeError:
        try:
            result = _repair_truncated_strategy_json(text)
        except json.JSONDecodeError:
            logger.warning("Adaptive strategist response not valid JSON: %.500s", text)
            return {**_EMPTY_STRATEGY, "rationale": "JSON parse failed"}

    action_groups = result.get("action_groups", [])
    if not isinstance(action_groups, list):
        action_groups = []

    # Tier 2.5: AG scope post-validator. Enforce one-cluster-per-AG by
    # keeping only the highest-impact source cluster when the LLM returns
    # multiple clusters with different root_causes. AGs with a single
    # cluster pass through unchanged. This prevents scope-creep AGs like
    # iteration 2 (3 clusters x 4 levers x 27 patches) where a single bad
    # patch takes down the other 26 when the iteration rolls back.
    _all_clusters_map: dict[str, dict] = {
        c.get("cluster_id", ""): c for c in list(clusters) + list(soft_signal_clusters) if c.get("cluster_id")
    }

    def _cluster_namespace(_cid: str) -> str:
        """T2.16: H vs S namespace for a cluster id (backward-compatible with bare C###)."""
        if not _cid:
            return ""
        _ch = _cid[:1].upper()
        return _ch if _ch in ("H", "S") else ""

    _validated_ags: list[dict] = []
    for _ag in action_groups:
        if not isinstance(_ag, dict):
            continue
        # T2.16: normalise primary/secondary cluster IDs. LLMs that still
        # emit ``source_cluster_ids`` (legacy) are auto-migrated: the
        # first entry becomes primary, the rest secondaries. We also
        # refuse implicit cross-namespace spans — if primary is H### but
        # a secondary is S### (or vice versa) and the LLM did not
        # explicitly declare secondary_cluster_ids, drop the cross-ns
        # entries and log it.
        _primary_id = str(_ag.get("primary_cluster_id") or "").strip()
        _secondary_ids_raw = _ag.get("secondary_cluster_ids") or []
        if not _primary_id:
            _legacy_ids = [str(x) for x in (_ag.get("source_cluster_ids") or []) if x]
            if _legacy_ids:
                _primary_id = _legacy_ids[0]
                if len(_legacy_ids) > 1 and not _secondary_ids_raw:
                    _secondary_ids_raw = _legacy_ids[1:]
                    logger.info(
                        "T2.16: migrated legacy source_cluster_ids=%s -> "
                        "primary=%s, secondary=%s",
                        _legacy_ids, _primary_id, _secondary_ids_raw,
                    )
        if _primary_id:
            _ag["primary_cluster_id"] = _primary_id
            _primary_ns = _cluster_namespace(_primary_id)
            _kept_secondaries: list[str] = []
            _explicit_secondary = bool(_ag.get("secondary_cluster_ids"))
            for _sid in _secondary_ids_raw:
                _sid_str = str(_sid).strip()
                if not _sid_str or _sid_str == _primary_id:
                    continue
                _sid_ns = _cluster_namespace(_sid_str)
                if _sid_ns and _primary_ns and _sid_ns != _primary_ns and not _explicit_secondary:
                    logger.warning(
                        "T2.16: dropped implicit cross-namespace secondary %s "
                        "(primary=%s, namespaces differ). Declare secondary_cluster_ids "
                        "explicitly to span hard+soft.",
                        _sid_str, _primary_id,
                    )
                    continue
                _kept_secondaries.append(_sid_str)
            if _kept_secondaries:
                _ag["secondary_cluster_ids"] = _kept_secondaries
            elif "secondary_cluster_ids" in _ag:
                _ag["secondary_cluster_ids"] = []
            # Mirror into source_cluster_ids so all downstream back-fill
            # and printer code paths keep working without duplication.
            _ag["source_cluster_ids"] = [_primary_id] + _kept_secondaries
        _src_ids = list(_ag.get("source_cluster_ids") or [])
        if len(_src_ids) <= 1:
            _validated_ags.append(_ag)
            continue
        _src_clusters = [
            _all_clusters_map.get(str(cid))
            for cid in _src_ids
            if _all_clusters_map.get(str(cid))
        ]
        if not _src_clusters:
            _validated_ags.append(_ag)
            continue
        _winner = max(_src_clusters, key=cluster_impact)
        _compatible_clusters = [
            c for c in _src_clusters
            if c is _winner or clusters_share_defect_identity(_winner, c)
        ]
        if len(_compatible_clusters) < len(_src_clusters):
            _dropped_ids = [
                str(c.get("cluster_id", ""))
                for c in _src_clusters
                if c not in _compatible_clusters
            ]
            logger.warning(
                "AG scope bound (RCA defect identity): dropped %d incompatible "
                "source cluster(s) %s — kept defect-compatible clusters %s",
                len(_dropped_ids),
                _dropped_ids,
                [c.get("cluster_id", "") for c in _compatible_clusters],
            )
        _ag["source_cluster_ids"] = [
            str(c.get("cluster_id", ""))
            for c in _compatible_clusters
            if c.get("cluster_id")
        ]
        _ag["affected_questions"] = sorted({
            str(q)
            for c in _compatible_clusters
            for q in (c.get("question_ids", []) or [])
            if str(q)
        })
        _validated_ags.append(_ag)
        continue
    action_groups = _validated_ags

    global_rewrite = _normalize_instruction_rewrite(result.get("global_instruction_rewrite"))
    rationale = result.get("rationale", "")

    _rewrite_preview = _preview_instruction_rewrite(global_rewrite)
    _rewrite_desc = (
        f"{len(global_rewrite)} sections" if isinstance(global_rewrite, dict)
        else f"{len(global_rewrite)} chars"
    )
    logger.info(
        "\n┌─── LLM Response [ADAPTIVE STRATEGIST] ──────────────────────────────\n"
        "│ Action groups: %d\n"
        "│ Global instruction rewrite: %s\n"
        "│ Rationale: %s\n"
        "└─────────────────────────────────────────────────────────────────────────",
        len(action_groups),
        _rewrite_desc,
        _truncate_on_boundary(str(rationale), 300),
    )

    # Tier 2.1 + 2.12: back-fill ``affected_questions`` from
    # ``source_cluster_ids`` when the LLM omitted it. Prefer
    # ``base_question_ids`` (real benchmark ids) over the internal
    # ``question_ids`` (which may carry :vN suffixes) so outward-facing
    # AG fields never leak synthetic tokens.
    #
    # T1.10: also populate ``affected_base_question_ids`` on every AG —
    # downstream code that gates pass/fail accounting (slice sampler,
    # persistence counter) uses base qids, while row-targeting uses the
    # suffixed ``affected_questions``. Keeping both lists on the AG is
    # cheap and eliminates the last source of ``_003`` vs ``_003:v2``
    # confusion across hard/soft passes.
    _all_clusters = list(clusters) + list(soft_signal_clusters)
    _clusters_by_id = {
        c.get("cluster_id", ""): c for c in _all_clusters if c.get("cluster_id")
    }
    for _ag in action_groups:
        if not isinstance(_ag, dict):
            continue
        _source_cids = _ag.get("source_cluster_ids") or []
        _base_qids_union: set[str] = set()
        _suffixed_qids_union: set[str] = set()
        for _cid in _source_cids:
            _src = _clusters_by_id.get(str(_cid))
            if _src:
                _base_qids_union.update(_src.get("base_question_ids") or [])
                _suffixed_qids_union.update(_src.get("question_ids") or [])
        if _base_qids_union and not _ag.get("affected_base_question_ids"):
            _ag["affected_base_question_ids"] = sorted(_base_qids_union)

        # T2.16: primary-cluster coverage assertion. After backfill, the
        # AG's affected_base_question_ids MUST be a superset of the
        # primary cluster's base_question_ids. If the LLM returned a
        # shrunk list (e.g. AG1 with primary=H001 but only [_001, _006]
        # while H001 covers {_001, _003, _007, _009}), back-fill the
        # missing entries and log a warning so it's visible in runs.
        _primary_id_check = str(_ag.get("primary_cluster_id") or "").strip()
        if _primary_id_check:
            _primary_cluster = _clusters_by_id.get(_primary_id_check)
            if _primary_cluster:
                _primary_base = set(_primary_cluster.get("base_question_ids") or [])
                _current_base = set(_ag.get("affected_base_question_ids") or [])
                _missing = _primary_base - _current_base
                if _missing:
                    _ag["affected_base_question_ids"] = sorted(_current_base | _primary_base)
                    logger.warning(
                        "T2.16: AG affected_base_question_ids did not cover primary "
                        "cluster %s — added %d missing base qid(s) %s",
                        _primary_id_check, len(_missing), sorted(_missing),
                    )

        if _ag.get("affected_questions"):
            continue
        _qids: set[str] = set(_base_qids_union) or set(_suffixed_qids_union)
        if _qids:
            _ag["affected_questions"] = sorted(_qids)
            logger.info(
                "Back-filled ag.affected_questions from source_cluster_ids=%s "
                "(%d base_question_id(s))",
                list(_source_cids), len(_qids),
            )

    # ── v2 Task 12: deterministic conditional-disambiguation emission ──
    # When the strategist LLM did not surface a collision the detector
    # found, attach a deterministic conditional-disambiguation patch to
    # the AG whose affected questions overlap the collision; otherwise
    # fall back to the first AG so the rule is never silently dropped.
    if intent_collisions:
        from genie_space_optimizer.optimization.intent_disambiguation import (
            build_conditional_disambiguation_patch,
        )
        addressed_terms = {
            str(p.get("term") or "").strip()
            for ag in action_groups for p in (ag.get("proposed_patches") or [])
            if p.get("type") == "add_conditional_disambiguation_instruction"
        }
        representatives_by_qid: dict[str, str] = {}
        for c in clusters:
            rep = c.get("representative_question") or ""
            for q in c.get("question_ids") or []:
                if rep and q not in representatives_by_qid:
                    representatives_by_qid[str(q)] = str(rep)
        for collision in intent_collisions:
            if collision["term"] in addressed_terms:
                continue
            patch = build_conditional_disambiguation_patch(
                collision=collision,
                representatives=representatives_by_qid,
                proposal_id=f"P_INTENT_{collision['term']}",
            )
            for ag in action_groups:
                shared_qids = set(patch["target_qids"]) & set(
                    ag.get("affected_questions") or []
                )
                if shared_qids:
                    ag.setdefault("proposed_patches", []).append(patch)
                    break
            else:
                if action_groups:
                    action_groups[0].setdefault("proposed_patches", []).append(patch)

    _out_lines = [
        f"\n{'=' * _W}",
        f"  STRATEGIST OUTPUT (Iteration {_iter_label})",
        f"{'=' * _W}",
    ]
    if action_groups:
        ag = action_groups[0]
        levers = sorted(ag.get("lever_directives", {}).keys())
        qs = ag.get("affected_questions", [])
        _out_lines.append(f"|  {'AG:':<28s} {ag.get('root_cause_summary', '?')[:100]}")
        _out_lines.append(f"|  {'Levers:':<28s} {', '.join(levers)}")
        _out_lines.append(f"|  {'Affected Questions:':<28s} {len(qs)} — {', '.join(qs[:5])}")
        _out_lines.append(f"|  {'Escalation:':<28s} {ag.get('escalation', 'none') or 'none'}")
        _out_lines.append(f"|  {'Rationale:':<28s} {_truncate_on_boundary(str(rationale), 200)}")
        if global_rewrite:
            _out_lines.append(f"|  {'Instruction Rewrite:':<28s} {_rewrite_preview}")
    else:
        _out_lines.append("|  No action group produced")
        if rationale:
            _out_lines.append(f"|  {'Rationale:':<28s} {_truncate_on_boundary(str(rationale), 200)}")
    _out_lines.append(f"{'=' * _W}")
    print("\n".join(_out_lines))

    return {
        "action_groups": action_groups[:MAX_ACTION_GROUPS_PER_STRATEGY],
        "global_instruction_rewrite": global_rewrite,
        "rationale": rationale,
    }


def _call_llm_for_stage_1_discovery(
    ag_id: str,
    root_cause_summary: str,
    clusters: list[dict],
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict:
    """Plan 3 — Stage-1 discovery LLM call.

    Picks ``applicable_skills`` from the Plan 1 catalogue for ONE
    action group. Returns ``{"applicable_skills": [...], "discovery_rationale": str}``.

    Best-effort: any failure (LLM exception, JSON parse failure,
    no valid skill_ids) returns empty ``applicable_skills`` and the
    caller falls back to ``_call_llm_for_adaptive_strategy``.

    Capture-sink hit recorded when split or shadow flag is on.
    """
    from genie_space_optimizer.common.config import (
        STAGE_1_DISCOVERY_MAX_TOKENS,
        STAGE_1_DISCOVERY_PROMPT,
        _THREE_STAGE_SKILL_NAMES,
    )
    from genie_space_optimizer.optimization.evaluation import (
        _extract_json,
        _link_prompt_to_trace,
    )

    config = metadata_snapshot.get("config") or {}
    space_desc = config.get("description") or "(No description set.)"
    if isinstance(space_desc, list):
        space_desc = "\n".join(space_desc)

    cluster_briefs = _format_cluster_briefs_afs(clusters or [], top_n=5)

    # Plan 11: the three_stage_pipeline rendering helpers below are
    # prompt-construction utilities reused by Stage 1 discovery for
    # the legacy strategist. Plan 11's diagnose/cluster/synthesize
    # stages have their own prompts and do not invoke this discovery
    # path, but reuse the same skill catalogue — plan11_llm_first_enabled()
    # has no impact on the rendered output here.
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_failure_type_routing_table,
        _render_rich_skill_catalogue,
    )
    skill_catalogue = _render_rich_skill_catalogue()
    failure_type_routing_table = _render_failure_type_routing_table()

    # Compute per-AG relevant_objects (union of blame_set FQNs across
    # clusters) for just-in-time allowlist filtering. Escape hatch:
    # GSO_STAGE_1_ALLOWLIST_FULL=1 bypasses the filter for debugging.
    if os.environ.get("GSO_STAGE_1_ALLOWLIST_FULL") == "1":
        relevant_objects: set[str] | None = None
    else:
        relevant_objects = set()
        for c in (clusters or []):
            for b in (c.get("asi_blame_set") or []):
                if isinstance(b, str) and b:
                    relevant_objects.add(b)
                elif isinstance(b, dict):
                    for key in ("fqn", "name", "identifier", "table"):
                        v = b.get(key)
                        if v:
                            relevant_objects.add(str(v))
                            break
        if not relevant_objects:
            # Empty blame extraction -> safest fallback is full allowlist.
            relevant_objects = None
    _allowlist = _build_identifier_allowlist(
        metadata_snapshot, relevant_objects=relevant_objects,
    )

    format_kwargs: dict[str, Any] = {
        "space_description": space_desc,
        "ag_id": ag_id,
        "root_cause_summary": root_cause_summary or "(unknown)",
        "cluster_briefs": cluster_briefs,
        "skill_catalogue": skill_catalogue,
        "failure_type_routing_table": failure_type_routing_table,
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
    }

    prompt = format_mlflow_template(STAGE_1_DISCOVERY_PROMPT, **format_kwargs)
    _link_prompt_to_trace("stage_1_discovery")

    from genie_space_optimizer.common.config import (
        _record_three_stage_discovery_call,
    )
    _record_three_stage_discovery_call(ag_id)

    logger.info(
        "\n┌─── LLM Call [STAGE_1_DISCOVERY] ─────────────────────────────────\n"
        "│ AG: %s  Clusters: %d  Prompt: %d chars\n"
        "└─────────────────────────────────────────────────────────────────────",
        ag_id, len(clusters or []), len(prompt),
    )

    system_msg = (
        "You are a JSON API. You MUST respond with ONLY a valid JSON "
        "object containing 'applicable_skills' (array) and "
        "'discovery_rationale' (string). No prose outside the JSON."
    )

    from genie_space_optimizer.optimization.prompt_io import (
        Stage1DiscoveryOutput,
    )
    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name="stage_1_discovery",
            max_tokens=STAGE_1_DISCOVERY_MAX_TOKENS,
            response_model=Stage1DiscoveryOutput,
        )
    except Exception:
        logger.warning(
            "Stage-1 discovery LLM call failed for AG=%s — "
            "falling back to legacy strategist", ag_id, exc_info=True,
        )
        return {"applicable_skills": [], "discovery_rationale": "LLM call failed"}

    try:
        result = _extract_json(text, strict=True)
    except (json.JSONDecodeError, ValueError):
        logger.warning(
            "Stage-1 discovery returned non-JSON (AG=%s): %.300s",
            ag_id, text,
        )
        return {"applicable_skills": [], "discovery_rationale": "JSON parse failed"}
    if not isinstance(result, dict):
        logger.warning(
            "Stage-1 discovery returned non-object JSON (AG=%s): %.300s",
            ag_id, text,
        )
        return {"applicable_skills": [], "discovery_rationale": "JSON parse failed"}

    raw_picks = result.get("applicable_skills", [])
    if not isinstance(raw_picks, list):
        raw_picks = []

    # Plan 11: _coerce_target_objects_for_skill is a pure shape-coercion
    # utility (validates LLM-emitted target_objects against the skill's
    # frontmatter constraints). Plan 11's synthesize stage produces
    # target_objects on RepairProposal directly, so this coercion is
    # legacy-only; plan11_llm_first_enabled() does not change behavior here.
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _coerce_target_objects_for_skill,
    )
    from genie_space_optimizer.skills._loader import _SKILL_LOADER

    valid_picks: list[dict] = []
    for pick in raw_picks:
        if not isinstance(pick, dict):
            continue
        sid = pick.get("skill_id", "")
        if sid not in _THREE_STAGE_SKILL_NAMES:
            logger.info(
                "Stage-1 discovery dropped unknown skill_id=%s (AG=%s)",
                sid, ag_id,
            )
            continue

        # Per-skill target-shape coercion.
        try:
            skill_meta = _SKILL_LOADER.load_metadata(sid)
        except Exception:
            skill_meta = {}
        target_kind = str(skill_meta.get("target_kind", "")).strip()
        target_min_count = int(skill_meta.get("target_min_count", 0) or 0)
        raw_targets = pick.get("target_objects") or []
        if target_kind:
            coerced, dropped = _coerce_target_objects_for_skill(
                skill_id=sid,
                target_kind=target_kind,
                target_min_count=target_min_count,
                raw_targets=raw_targets,
                allowlist=_allowlist,
            )
            if coerced is None:
                logger.info(
                    "Stage-1 discovery dropped pick %s (AG=%s): "
                    "post-coercion target count below min_count=%d "
                    "(dropped=%s)",
                    sid, ag_id, target_min_count, dropped,
                )
                continue
            if dropped:
                logger.info(
                    "Stage-1 discovery coerced pick %s (AG=%s): "
                    "dropped %d mismatched target(s) %s",
                    sid, ag_id, len(dropped), dropped,
                )
            final_targets = coerced
        else:
            # No target_kind in frontmatter -> conservative pass-through.
            final_targets = list(raw_targets)

        valid_picks.append({
            "skill_id": sid,
            "target_objects": final_targets,
            "expected_impact_qids": pick.get("expected_impact_qids") or [],
            "evidence_refs": pick.get("evidence_refs") or [],
            "why": str(pick.get("why", "")),
            "priority": int(pick.get("priority", 3) or 3),
        })

    return {
        "applicable_skills": valid_picks,
        "discovery_rationale": str(result.get("discovery_rationale", "")),
    }


def _emit_three_stage_shadow_comparison(
    ag_id: str,
    stage_1_picks: list[dict],
    legacy_action_groups: list[dict],
    pipeline_stage_2_results: list[dict],
) -> None:
    """Plan 3 — emit one shadow-comparison record per AG.

    The 2026-05-16 dead-flag cleanup removed the live callers of this
    function (it was called from ``_select_strategy_path_for_iteration``
    only on the shadow branch, which is gone). The body stays
    callable so the comparison-math unit tests keep their direct entry
    point; the underlying ``_record_three_stage_shadow_comparison``
    sink no-ops unless ``GSO_THREE_STAGE_CAPTURE_PATH`` is set.

    Maps legacy ``lever_directives.keys()`` → canonical skill_ids
    using the static legacy→skill table below; computes set overlap
    between the mapped legacy keys and Stage-1 picks; records totals
    and per-skill proposal counts for the export script.
    """
    from genie_space_optimizer.common.config import (
        _record_three_stage_shadow_comparison,
    )

    # Legacy lever-directive key → canonical skill_id (one or many).
    # When a legacy key maps to multiple skills (lever 5 → 5a + 5b),
    # the legacy set absorbs both for the overlap calculation.
    legacy_to_skills: dict[str, set[str]] = {
        "1": {"lever-1-table-column-description"},
        "2": {"lever-2-mv-column-refinement"},
        "3": {"lever-3-tvf-routing"},
        "4": {"lever-4-join-discovery"},
        "5": {"lever-5a-instructions", "lever-5b-example-sql"},
        "6": {"lever-6-sql-expression"},
    }

    legacy_keys: set[str] = set()
    for ag in (legacy_action_groups or []):
        for k in (ag.get("lever_directives", {}) or {}).keys():
            legacy_keys.add(str(k))

    legacy_skill_set: set[str] = set()
    for k in legacy_keys:
        legacy_skill_set |= legacy_to_skills.get(k, set())

    stage_1_skill_set: set[str] = {
        p.get("skill_id", "") for p in (stage_1_picks or [])
        if p.get("skill_id")
    }

    union = legacy_skill_set | stage_1_skill_set
    overlap = (
        len(legacy_skill_set & stage_1_skill_set) / len(union)
        if union else 1.0
    )

    proposals_per_skill = {
        r.get("skill_id", ""): len(r.get("proposals") or [])
        for r in (pipeline_stage_2_results or [])
    }

    record = {
        "ag_id": ag_id,
        "stage_1_skill_ids": sorted(stage_1_skill_set),
        "legacy_lever_keys": sorted(legacy_keys),
        "legacy_skill_set_mapped": sorted(legacy_skill_set),
        "structural_overlap": overlap,
        "stage_1_skills_only": sorted(stage_1_skill_set - legacy_skill_set),
        "legacy_skills_only": sorted(legacy_skill_set - stage_1_skill_set),
        "pipeline_proposals_per_skill": proposals_per_skill,
        "pipeline_total_proposals": sum(proposals_per_skill.values()),
    }
    _record_three_stage_shadow_comparison(record)


# ── Phase 1a: Triage ────────────────────────────────────────────────────

_EMPTY_TRIAGE: dict[str, Any] = {
    "action_groups": [],
    "global_instruction_guidance": "",
    "rationale": "",
}


def _call_llm_for_triage(
    clusters: list[dict],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict:
    """Phase 1a: lightweight triage call that sees ALL clusters compactly.

    Returns action group *skeletons* with ``levers_needed``, ``focus_tables``,
    and ``focus_columns`` — no actual lever directives yet.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions
    from genie_space_optimizer.optimization.evaluation import (
        _extract_json,
        _link_prompt_to_trace,
    )

    schema_index = _format_schema_index(metadata_snapshot)
    cluster_summaries = _format_compact_cluster_summaries(clusters)
    soft_summary = _format_soft_signal_summary(soft_signal_clusters)
    join_summary = _format_join_specs_context(metadata_snapshot)
    current_instr = _get_general_instructions(metadata_snapshot) or "(No current instructions.)"
    instruction_summary = current_instr[:1500]
    if len(current_instr) > 1500:
        instruction_summary += f" ... ({len(current_instr) - 1500} chars omitted)"

    format_kwargs: dict[str, Any] = {
        "schema_index": schema_index,
        "cluster_summaries": cluster_summaries,
        "soft_signal_summary": soft_summary,
        "current_join_summary": join_summary,
        "instruction_summary": instruction_summary,
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, STRATEGIST_TRIAGE_PROMPT,
        priority_keys=["soft_signal_summary", "instruction_summary", "schema_index", "cluster_summaries"],
    )

    prompt = format_mlflow_template(STRATEGIST_TRIAGE_PROMPT, **format_kwargs)

    _W = 78
    logger.info(
        "\n┌─── LLM Call [TRIAGE] %s\n│ Clusters: %d hard, %d soft\n│ Prompt: %d chars\n└%s",
        "─" * max(0, _W - 23), len(clusters), len(soft_signal_clusters), len(prompt), "─" * (_W - 1),
    )
    print(
        f"\n{'=' * _W}\n"
        f"  PHASE 1a: TRIAGE STRATEGIST\n"
        f"  Clusters: {len(clusters)} hard, {len(soft_signal_clusters)} soft\n"
        f"  Prompt: {len(prompt):,} chars\n"
        f"{'=' * _W}"
    )

    _link_prompt_to_trace("strategist_triage")

    system_msg = (
        "You are a JSON API. Respond with ONLY a valid JSON object. "
        "No explanation or markdown outside the JSON. "
        "The JSON must contain an 'action_groups' array."
    )

    try:
        from genie_space_optimizer.optimization.prompt_io import (
            StrategistTriageOutput,
        )
        text, _response = _traced_llm_call(
            w, system_msg, prompt, span_name="phase_1a_triage",
            response_model=StrategistTriageOutput,
        )
    except Exception:
        logger.exception("Triage LLM call failed after retries (prompt len: %d)", len(prompt))
        return {**_EMPTY_TRIAGE, "rationale": "LLM call failed"}

    try:
        result = _extract_json(text)
    except json.JSONDecodeError:
        try:
            result = _repair_truncated_strategy_json(text)
        except json.JSONDecodeError:
            logger.warning("Triage LLM response was not valid JSON: %.500s", text)
            return {**_EMPTY_TRIAGE, "rationale": "JSON parse failed"}

    ags = result.get("action_groups", [])
    if not isinstance(ags, list):
        ags = []

    logger.info("Triage produced %d action group skeleton(s)", len(ags))
    print(f"\n  Triage produced {len(ags)} action group skeleton(s)")
    for i, ag in enumerate(ags):
        levers = ag.get("levers_needed", [])
        ft = ag.get("focus_tables", [])
        fc = ag.get("focus_columns", [])
        print(
            f"    AG{i + 1}: {ag.get('root_cause_summary', '?')[:80]}"
            f" | levers={levers} | tables={len(ft)} | cols={len(fc)}"
        )

    return {
        "action_groups": ags,
        "global_instruction_guidance": result.get("global_instruction_guidance", ""),
        "rationale": result.get("rationale", ""),
    }


# ── Phase 1b: AG Detail ─────────────────────────────────────────────────

def _call_llm_for_ag_detail(
    ag_skeleton: dict,
    clusters: list[dict],
    metadata_snapshot: dict,
    instruction_char_budget: int = 4000,
    w: WorkspaceClient | None = None,
) -> dict:
    """Phase 1b: produce full lever_directives for one action group skeleton.

    Receives the skeleton from triage plus *only* the relevant clusters and
    metadata scoped to ``focus_tables``/``focus_columns``.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions
    from genie_space_optimizer.optimization.evaluation import (
        _extract_json,
        _link_prompt_to_trace,
    )

    ag_id = ag_skeleton.get("id", "AG?")
    source_cids = set(ag_skeleton.get("source_cluster_ids", []))
    relevant_clusters = [c for c in clusters if c.get("cluster_id") in source_cids]
    if not relevant_clusters:
        relevant_clusters = clusters[:3]

    # Bug #4 (P2.2) — strategist detail context uses AFS structural_diff,
    # never raw expected_sql / generated_sql.
    from genie_space_optimizer.optimization.afs import format_afs as _format_afs_local
    sql_diffs_parts: list[str] = []
    for cluster in relevant_clusters:
        cid = cluster.get("cluster_id", "?")
        rc = cluster.get("root_cause", "unknown")
        sql_diffs_parts.append(f"### Cluster {cid}: {rc}")
        _afs_local = _format_afs_local(cluster)
        sql_diffs_parts.append(
            json.dumps(_afs_local.get("structural_diff", {}), default=str, indent=2)
        )
        sql_diffs_parts.append("")
    sql_diffs_text = "\n".join(sql_diffs_parts) if sql_diffs_parts else "(no SQL context)"

    focus_tables = ag_skeleton.get("focus_tables", [])
    focus_columns = ag_skeleton.get("focus_columns", [])
    blame_set = list(focus_tables) + [c.split(".")[-1] for c in focus_columns if "." in c]
    if not blame_set:
        for c in relevant_clusters:
            b = c.get("asi_blame_set")
            if isinstance(b, str) and b:
                blame_set.extend(b.split("|"))
            elif isinstance(b, list):
                blame_set.extend(str(x) for x in b)

    levers_needed = ag_skeleton.get("levers_needed", [1, 5])

    structured_table_ctx = _format_structured_table_context(
        metadata_snapshot, blame_set or None, lever=1,
    )
    structured_col_ctx = _format_structured_column_context(
        metadata_snapshot, blame_set or None, lever=1,
    )
    structured_fn_ctx = ""
    if 3 in levers_needed:
        structured_fn_ctx = _format_structured_function_context(metadata_snapshot, lever=3)

    join_specs = _format_join_specs_context(metadata_snapshot)
    current_instr = _get_general_instructions(metadata_snapshot) or "(No current instructions.)"
    example_sqls = _format_existing_example_sqls(metadata_snapshot)

    skeleton_json = json.dumps(ag_skeleton, indent=2, default=str)
    _allowlist = _build_identifier_allowlist(metadata_snapshot)

    format_kwargs: dict[str, Any] = {
        "action_group_skeleton": skeleton_json,
        "sql_diffs": sql_diffs_text,
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
        "structured_table_context": structured_table_ctx,
        "structured_column_context": structured_col_ctx,
        "structured_function_context": structured_fn_ctx,
        "current_join_specs": join_specs,
        "current_instructions": current_instr,
        "existing_example_sqls": example_sqls,
        "instruction_char_budget": instruction_char_budget,
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, STRATEGIST_DETAIL_PROMPT,
        priority_keys=["existing_example_sqls", "structured_function_context", "current_instructions", "sql_diffs"],
    )

    prompt = format_mlflow_template(STRATEGIST_DETAIL_PROMPT, **format_kwargs)

    logger.info(
        "\n┌─── LLM Call [AG DETAIL: %s] ────────────────────────────────────\n"
        "│ Clusters: %d | Levers: %s | Prompt: %d chars\n"
        "└─────────────────────────────────────────────────────────────────────",
        ag_id, len(relevant_clusters), levers_needed, len(prompt),
    )
    print(
        f"\n  Phase 1b: Detailing {ag_id} "
        f"({len(relevant_clusters)} clusters, levers={levers_needed}, "
        f"prompt={len(prompt):,} chars)"
    )

    _link_prompt_to_trace("strategist_detail")

    _EMPTY_DETAIL: dict[str, Any] = {
        "lever_directives": {}, "coordination_notes": "",
        "instruction_contribution": "", "proposals": [],
    }

    system_msg = (
        "You are a JSON API. Respond with ONLY a valid JSON object. "
        "No explanation or markdown outside the JSON. "
        "The JSON must contain a 'lever_directives' object."
    )

    try:
        from genie_space_optimizer.optimization.prompt_io import (
            StrategistDetailOutput,
        )
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name=f"phase_1b_detail_{ag_id}",
            response_model=StrategistDetailOutput,
        )
    except Exception:
        logger.exception("AG detail LLM call failed after retries for %s", ag_id)
        return dict(_EMPTY_DETAIL)

    try:
        result = _extract_json(text)
    except json.JSONDecodeError:
        try:
            result = _repair_truncated_strategy_json(text)
        except json.JSONDecodeError:
            logger.warning("AG detail LLM response not valid JSON: %.500s", text)
            return dict(_EMPTY_DETAIL)

    lever_dirs = result.get("lever_directives", {})
    if not isinstance(lever_dirs, dict):
        lever_dirs = {}
    coord = result.get("coordination_notes", "")
    raw_instr = result.get("instruction_contribution", "")
    if isinstance(raw_instr, dict):
        instr_contrib = raw_instr
        instr_len = sum(len(str(v)) for v in raw_instr.values())
    else:
        instr_contrib = str(raw_instr) if raw_instr else ""
        instr_len = len(instr_contrib)
    proposals = result.get("proposals", [])
    if not isinstance(proposals, list):
        proposals = []

    logger.info(
        "AG %s detail: %d lever directives, coordination=%d chars, instruction=%d chars, proposals=%d",
        ag_id, len(lever_dirs), len(coord), instr_len, len(proposals),
    )
    print(
        f"    {ag_id} detail: levers={sorted(lever_dirs.keys())}, "
        f"coordination={len(coord)} chars, instruction={instr_len} chars, "
        f"proposals={len(proposals)}"
    )

    return {
        "lever_directives": lever_dirs,
        "coordination_notes": coord,
        "instruction_contribution": instr_contrib,
        "proposals": proposals,
    }


def _generate_holistic_strategy(
    clusters: list[dict],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict:
    """Two-phase progressive strategist.

    Phase 1a — Triage: compact summaries of ALL clusters, produces action
    group skeletons with ``levers_needed`` and ``focus_objects``.

    Phase 1b — Detail: for each skeleton, full SQL diffs and structured
    metadata (scoped to focus objects) produce concrete ``lever_directives``.

    Falls back to the monolithic ``_call_llm_for_strategy`` if triage
    returns 0 action groups (LLM failure safety net).

    Returns a strategy dict with ``action_groups``, ``global_instruction_rewrite``,
    and ``rationale`` — identical shape to what harness.py expects.
    """
    import mlflow
    from mlflow.entities import SpanType

    from genie_space_optimizer.optimization.applier import _get_general_instructions

    hard = [c for c in clusters if c.get("cluster_id")]
    soft = [c for c in soft_signal_clusters if c.get("cluster_id")]

    if not hard and not soft:
        logger.info("No clusters to strategize on — returning empty strategy")
        return {**_EMPTY_STRATEGY, "rationale": "No clusters available"}

    with mlflow.start_span(name="generate_holistic_strategy", span_type=SpanType.CHAIN) as span:
        span.set_inputs({
            "hard_clusters": len(hard),
            "soft_clusters": len(soft),
        })

        # ── Phase 1a: Triage (CHAT_MODEL span created inside _call_llm_for_triage)
        triage_result = _call_llm_for_triage(
            clusters=hard,
            soft_signal_clusters=soft,
            metadata_snapshot=metadata_snapshot,
            w=w,
        )
        triage_ags = triage_result.get("action_groups", [])

        if not triage_ags:
            logger.warning(
                "Triage returned 0 action groups — falling back to monolithic strategist"
            )
            print("\n  Triage returned 0 AGs — falling back to monolithic call")
            fallback = _call_llm_for_strategy(
                clusters=hard,
                soft_signal_clusters=soft,
                metadata_snapshot=metadata_snapshot,
                w=w,
            )
            ags = fallback.get("action_groups", [])
            for i, ag in enumerate(ags):
                if "id" not in ag:
                    ag["id"] = f"AG{i + 1}"
                if "priority" not in ag:
                    ag["priority"] = i + 1
            span.set_outputs({
                "action_groups_count": len(ags),
                "mode": "monolithic_fallback",
                "global_instruction_rewrite_len": len(fallback.get("global_instruction_rewrite", "")),
            })
            return fallback

        # ── Phase 1b: Detail per AG (CHAT_MODEL spans created inside _call_llm_for_ag_detail)
        per_ag_budget = max(2000, 24000 // max(len(triage_ags), 1))
        final_ags: list[dict] = []
        str_contributions: list[str] = []
        dict_contributions: list[dict[str, str]] = []

        for i, skeleton in enumerate(triage_ags):
            if "id" not in skeleton:
                skeleton["id"] = f"AG{i + 1}"
            if "priority" not in skeleton:
                skeleton["priority"] = i + 1

            detail = _call_llm_for_ag_detail(
                ag_skeleton=skeleton,
                clusters=hard,
                metadata_snapshot=metadata_snapshot,
                instruction_char_budget=per_ag_budget,
                w=w,
            )

            lever_dirs = detail.get("lever_directives", {})
            coord_notes = detail.get("coordination_notes", "")
            instr_contrib = detail.get("instruction_contribution", "")
            proposals = detail.get("proposals", [])

            assembled_ag: dict[str, Any] = {
                "id": skeleton["id"],
                "root_cause_summary": skeleton.get("root_cause_summary", ""),
                "source_cluster_ids": skeleton.get("source_cluster_ids", []),
                "affected_questions": skeleton.get("affected_questions", []),
                "priority": skeleton.get("priority", i + 1),
                "lever_directives": lever_dirs,
                "coordination_notes": coord_notes,
                "proposals": proposals,
            }
            final_ags.append(assembled_ag)

            if isinstance(instr_contrib, dict) and instr_contrib:
                dict_contributions.append(instr_contrib)
            elif isinstance(instr_contrib, str) and instr_contrib.strip():
                str_contributions.append(instr_contrib)

        # ── Merge instruction contributions (structure-aware) ────────
        global_guidance = triage_result.get("global_instruction_guidance", "")
        existing_instr = _get_general_instructions(metadata_snapshot)

        if dict_contributions:
            existing_sections = _ensure_structured(
                existing_instr, metadata_snapshot, w=w,
            )
            merged_sections: dict[str, list[str]] = {
                s: list(existing_sections.get(s, []))
                for s in INSTRUCTION_SECTION_ORDER
            }
            valid_keys = set(INSTRUCTION_SECTION_ORDER)
            for dc in dict_contributions:
                for key, value in dc.items():
                    if key not in valid_keys:
                        continue
                    if value == "":
                        merged_sections[key] = []
                    elif isinstance(value, str):
                        merged_sections[key] = [
                            ln for ln in value.splitlines() if ln.strip()
                        ]
            parts: list[str] = []
            for section in INSTRUCTION_SECTION_ORDER:
                lines = merged_sections[section]
                if not lines:
                    continue
                parts.append(f"{section}:")
                for ln in lines:
                    s = ln.strip()
                    if not s:
                        continue
                    if not s.startswith("- "):
                        s = f"- {s}"
                    parts.append(s)
                parts.append("")
            global_rewrite = _sanitize_plaintext_instructions("\n".join(parts).strip())

            if existing_instr and not _instruction_coverage(
                existing_instr, global_rewrite
            ):
                logger.warning(
                    "Two-phase holistic rewrite drops key phrases — force-merging"
                )
                global_rewrite = _merge_structured_instructions(
                    existing=existing_instr,
                    contributions=[global_rewrite],
                )
        elif str_contributions or global_guidance:
            global_rewrite = _merge_structured_instructions(
                existing=existing_instr,
                contributions=str_contributions,
                global_guidance=global_guidance,
            )
        else:
            global_rewrite = ""

        if global_rewrite and len(global_rewrite) > MAX_HOLISTIC_INSTRUCTION_CHARS:
            global_rewrite = global_rewrite[:MAX_HOLISTIC_INSTRUCTION_CHARS]
            logger.warning(
                "Merged instruction rewrite truncated to %d chars",
                MAX_HOLISTIC_INSTRUCTION_CHARS,
            )

        strategy: dict[str, Any] = {
            "action_groups": final_ags,
            "global_instruction_rewrite": global_rewrite,
            "rationale": triage_result.get("rationale", ""),
        }

        span.set_outputs({
            "action_groups_count": len(final_ags),
            "mode": "two_phase_progressive",
            "global_instruction_rewrite_len": len(global_rewrite),
            "rationale": str(strategy.get("rationale", ""))[:300],
        })

        print(
            f"\n  Strategy complete: {len(final_ags)} action group(s), "
            f"{len(global_rewrite)} chars instruction rewrite"
        )

    return strategy


def validate_join_spec_types(
    join_spec: dict,
    metadata_snapshot: dict,
) -> tuple[bool, str]:
    """Validate that join columns have compatible data types.

    Parses the ``sql`` array to extract column names from the join condition,
    looks up their ``data_type`` in the enriched metadata, and checks
    compatibility.

    Returns ``(valid, reason)`` — if valid is False, *reason* explains why.
    """
    sql_parts = join_spec.get("sql", [])
    if not sql_parts:
        return True, "no sql to validate"

    condition = sql_parts[0] if isinstance(sql_parts, list) else str(sql_parts)

    left_obj = join_spec.get("left", {})
    right_obj = join_spec.get("right", {})
    left_alias = left_obj.get("alias", "") if isinstance(left_obj, dict) else ""
    right_alias = right_obj.get("alias", "") if isinstance(right_obj, dict) else ""
    left_ident = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
    right_ident = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""

    # Parse "= " join conditions: `left_alias`.`col` = `right_alias`.`col`
    pattern = r"`([^`]+)`\s*\.\s*`([^`]+)`\s*=\s*`([^`]+)`\s*\.\s*`([^`]+)`"
    match = re.search(pattern, condition)
    if not match:
        return True, "could not parse join condition columns"

    cond_left_alias, cond_left_col = match.group(1), match.group(2)
    cond_right_alias, cond_right_col = match.group(3), match.group(4)

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    col_types: dict[tuple[str, str], str] = {}
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "") or tbl.get("name", "")
        short = _short_name(ident).lower()
        fqn_lower = ident.lower()
        for cc in tbl.get("column_configs", tbl.get("columns", [])):
            col_name = (cc.get("column_name") or cc.get("name", "")).lower()
            dt = cc.get("data_type", "")
            if col_name and dt:
                col_types[(short, col_name)] = str(dt).upper()
                col_types[(fqn_lower, col_name)] = str(dt).upper()

    left_type = (
        col_types.get((left_ident.lower(), cond_left_col.lower()), "")
        or col_types.get((cond_left_alias.lower(), cond_left_col.lower()), "")
    )
    right_type = (
        col_types.get((right_ident.lower(), cond_right_col.lower()), "")
        or col_types.get((cond_right_alias.lower(), cond_right_col.lower()), "")
    )

    if not left_type or not right_type:
        return True, "type info unavailable, skipping validation"

    if _types_compatible(left_type, right_type):
        return True, f"types compatible: {left_type} ↔ {right_type}"

    return False, (
        f"incompatible types: {cond_left_alias}.{cond_left_col} ({left_type}) "
        f"↔ {cond_right_alias}.{cond_right_col} ({right_type})"
    )


def _is_generic_counterfactual(fix: str) -> bool:
    """Return True if the counterfactual_fix is too vague to drive optimization."""
    if not fix:
        return True
    lower = fix.strip().lower()
    if any(lower.startswith(p) for p in GENERIC_FIX_PREFIXES):
        has_specific_ref = any(
            tok in lower
            for tok in (".", "_", "column", "table ", "tvf", "function ")
            if len(tok) > 1
        )
        if not has_specific_ref:
            return True
    return False


def _describe_fix(cluster: dict) -> str:
    """Describe the fix for a cluster, preferring specific ASI fixes."""
    asi_fixes = [f for f in cluster.get("asi_counterfactual_fixes", []) if f]
    specific_fixes = [f for f in asi_fixes if not _is_generic_counterfactual(f)]
    if specific_fixes:
        return specific_fixes[0]
    if cluster.get("root_cause") == "repeatability_issue":
        dominant_asset = cluster.get("asi_blame_set") or cluster.get(
            "dominant_asset", "TABLE"
        )
        base = REPEATABILITY_FIX_BY_ASSET.get(
            dominant_asset, REPEATABILITY_FIX_BY_ASSET["TABLE"]
        )
        return f"{base} (affects {len(cluster['question_ids'])} questions)"
    wrong_clause = cluster.get("asi_wrong_clause") or ""
    blame = cluster.get("asi_blame_set") or ""
    if wrong_clause and blame:
        return (
            f"Fix {cluster['root_cause']}: wrong clause '{wrong_clause}' "
            f"in {blame} affecting {len(cluster['question_ids'])} questions."
        )
    return (
        f"Fix {cluster['root_cause']} affecting "
        f"{len(cluster['question_ids'])} questions. "
        f"Judge: {cluster['affected_judge']}."
    )


def _deduplicate_clusters(clusters: list[dict]) -> list[dict]:
    """Merge clusters with identical (root_cause, question_ids) into one representative.

    Keeps the cluster with the highest confidence and tracks all merged judge
    names so attribution is not lost.
    """
    seen: dict[tuple, dict] = {}
    for c in clusters:
        key = (c.get("root_cause", ""), tuple(sorted(c.get("question_ids", []))))
        if key not in seen or c.get("confidence", 0) > seen[key].get("confidence", 0):
            merged = dict(c)
            merged.setdefault("merged_judges", [])
            seen[key] = merged
        seen[key]["merged_judges"].append(c.get("affected_judge", ""))
    return list(seen.values())


_INSTRUCTION_CONTENT_PATTERNS = re.compile(
    r"(?i)(ROUTING\s*RULES|MUST\s+follow|MUST\s+use|TVF\s+ROUTING|"
    r"METRIC\s+VIEW|MEASURE\s*\(|GROUP\s+BY|ORDER\s+BY|WHERE\s+.*=|"
    r"SELECT\s+\*\s+FROM|CRITICAL\s+ROUTING|enable_format_assistance|"
    r"enable_entity_matching)",
)


def _detect_instruction_content_in_description(
    metadata_snapshot: dict,
) -> list[dict]:
    """Check if the Genie Space description contains instruction-like content.

    Returns a list of ``update_description`` proposals that strip the
    instruction content, keeping only the user-facing summary paragraph.
    """
    config = metadata_snapshot.get("config") or {}
    desc = config.get("description") or ""
    if isinstance(desc, list):
        desc = "\n".join(desc)
    if not desc or not _INSTRUCTION_CONTENT_PATTERNS.search(desc):
        return []

    paragraphs = desc.split("\n\n")
    summary_parts: list[str] = []
    for para in paragraphs:
        if _INSTRUCTION_CONTENT_PATTERNS.search(para):
            break
        summary_parts.append(para.strip())
    clean_desc = "\n\n".join(summary_parts).strip()
    if not clean_desc or clean_desc == desc.strip():
        return []

    logger.info(
        "Description contains instruction-like content (%d chars). "
        "Proposing cleanup to %d chars.",
        len(desc),
        len(clean_desc),
    )
    return [
        {
            "patch_type": "update_description",
            "scope": "genie_config",
            "target": "genie_space",
            "proposed_value": clean_desc,
            "old_value": desc,
            "rationale": (
                "Description contained LLM-facing routing rules and SQL patterns. "
                "Stripped to user-facing summary only."
            ),
            "lever": 5,
            "net_impact": 1,
            "question_ids": [],
            "asi": {
                "failure_type": "missing_instruction",
                "severity": "minor",
                "counterfactual_fixes": [],
                "ambiguity_detected": False,
            },
        }
    ]


def _validate_lever5_proposals(
    proposals: list[dict],
    metadata_snapshot: dict,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
) -> list[dict]:
    """Filter out empty, generic, over-length, or hallucinated Lever 5 proposals.

    Bug #4 firewall — when ``benchmarks`` is provided, every proposal is
    additionally gated by ``is_benchmark_leak``. Proposals whose text or SQL
    fields match any benchmark at n-gram >= 0.60 (or whose SQL fingerprint
    matches exactly) are rejected. Callers that omit benchmarks degrade to
    the pre-Bug-#4 behaviour with a logged warning.
    """
    from genie_space_optimizer.common.config import MAX_INSTRUCTION_TEXT_CHARS
    from genie_space_optimizer.common.genie_schema import count_instruction_slots, MAX_INSTRUCTION_SLOTS

    bug4_corpus = None
    if benchmarks:
        from genie_space_optimizer.optimization.leakage import BenchmarkCorpus
        bug4_corpus = BenchmarkCorpus.from_benchmarks(benchmarks)
    else:
        logger.warning(
            "_validate_lever5_proposals called without benchmarks — "
            "Bug #4 firewall skipped. Caller should pass benchmarks.",
        )

    _tables = metadata_snapshot.get("tables") or []
    _funcs = metadata_snapshot.get("functions") or []
    _mvs = metadata_snapshot.get("metric_views") or []
    known_assets: set[str] = set()
    for t in _tables:
        name = (t.get("name") or t.get("identifier", "")).lower()
        known_assets.add(name)
        known_assets.add(name.rsplit(".", 1)[-1])
    for f in _funcs:
        name = (f.get("name") or f.get("identifier", "")).lower()
        known_assets.add(name)
        known_assets.add(name.rsplit(".", 1)[-1])
    for m in _mvs:
        name = (m.get("name") or m.get("identifier", "")).lower()
        known_assets.add(name)
        known_assets.add(name.rsplit(".", 1)[-1])
    for t in _tables:
        for c in t.get("column_configs", []) or t.get("columns", []) or []:
            if not isinstance(c, dict):
                continue
            col_name = str(c.get("name") or c.get("identifier") or "").strip()
            if col_name:
                known_assets.add(col_name.lower())
                known_assets.add(col_name.rsplit(".", 1)[-1].lower())
    known_assets.discard("")

    from genie_space_optimizer.optimization.instruction_publishability import (
        validate_publishable_instruction_text,
    )

    id_allowlist = _build_identifier_allowlist(metadata_snapshot)

    existing_eqs_raw = _get_existing_example_sqls(metadata_snapshot)
    existing_questions: set[str] = set()
    for e in existing_eqs_raw:
        if isinstance(e, dict):
            q = e.get("question", "")
            if isinstance(q, list):
                q = " ".join(q)
            q = q.lower().strip()
            if q:
                existing_questions.add(q)

    seen_new_questions: set[str] = set()

    current_slots = count_instruction_slots(metadata_snapshot)
    remaining_budget = MAX_INSTRUCTION_SLOTS - current_slots
    added_slots = 0

    valid: list[dict] = []
    for p in proposals:
        ptype = p.get("patch_type", "")
        if ptype not in ("add_instruction", "add_example_sql", "rewrite_instruction"):
            valid.append(p)
            continue

        if ptype == "rewrite_instruction":
            text = (p.get("proposed_value") or "").strip()
            if not text:
                logger.info("Rejecting empty rewrite_instruction proposal")
                continue
            if len(text) > MAX_HOLISTIC_INSTRUCTION_CHARS:
                logger.warning(
                    "Rejecting rewrite_instruction exceeding %d chars (%d chars)",
                    MAX_HOLISTIC_INSTRUCTION_CHARS, len(text),
                )
                continue
            _MIN_INSTRUCTION_LEN = 50
            if len(text) < _MIN_INSTRUCTION_LEN:
                logger.warning(
                    "Rejecting rewrite_instruction below minimum length (%d < %d chars)",
                    len(text), _MIN_INSTRUCTION_LEN,
                )
                continue
            found_sections = [
                s for s in INSTRUCTION_SECTION_ORDER
                if re.search(rf'^{re.escape(s)}:', text, re.MULTILINE)
            ]
            if not found_sections:
                logger.warning(
                    "Rejecting rewrite_instruction with no recognized structured sections "
                    "(expected ALL-CAPS headers like PURPOSE:, ASSET ROUTING:, etc.)."
                )
                continue
            publishability = validate_publishable_instruction_text(
                text,
                known_assets=known_assets,
            )
            if not publishability.ok:
                logger.warning(
                    "Rejecting non-publishable rewrite_instruction (%s): %.120s",
                    ",".join(publishability.reasons),
                    text,
                )
                continue
            valid.append(p)
            continue

        if ptype == "add_instruction":
            text = (p.get("proposed_value") or p.get("new_text") or "").strip()
            if not text:
                logger.info("Rejecting empty add_instruction proposal")
                continue
            if len(text) > MAX_INSTRUCTION_TEXT_CHARS:
                logger.warning(
                    "Rejecting add_instruction proposal exceeding %d chars (%d chars)",
                    MAX_INSTRUCTION_TEXT_CHARS,
                    len(text),
                )
                continue
            text_lower = text.lower()
            if known_assets and not any(a in text_lower for a in known_assets):
                logger.warning(
                    "Rejecting generic add_instruction (no known asset referenced): %.100s...",
                    text,
                )
                continue
            publishability = validate_publishable_instruction_text(
                text,
                known_assets=known_assets,
            )
            if not publishability.ok:
                logger.warning(
                    "Rejecting non-publishable add_instruction (%s): %.120s",
                    ",".join(publishability.reasons),
                    text,
                )
                continue

        if ptype == "add_example_sql":
            eq = (p.get("example_question") or "").strip()
            es = (p.get("example_sql") or "").strip()
            if not eq or not es:
                logger.info("Rejecting empty add_example_sql proposal")
                continue

            if added_slots >= remaining_budget:
                logger.warning(
                    "Dropping add_example_sql — slot budget exhausted (%d/%d)",
                    current_slots + added_slots, MAX_INSTRUCTION_SLOTS,
                )
                continue

            eq_norm = eq.lower().strip()
            if eq_norm in existing_questions:
                logger.info("Rejecting add_example_sql duplicate of existing config: %.80s", eq)
                continue
            if any(_ngram_similarity(eq_norm, eq_existing) > _EXAMPLE_SQL_SIMILARITY_THRESHOLD
                   for eq_existing in existing_questions):
                logger.info("Rejecting add_example_sql fuzzy-duplicate of existing config: %.80s", eq)
                continue
            if eq_norm in seen_new_questions:
                logger.info("Rejecting add_example_sql duplicate within batch: %.80s", eq)
                continue
            if any(_ngram_similarity(eq_norm, seen) > _EXAMPLE_SQL_SIMILARITY_THRESHOLD
                   for seen in seen_new_questions):
                logger.info("Rejecting add_example_sql fuzzy-duplicate within batch: %.80s", eq)
                continue
            seen_new_questions.add(eq_norm)

            sql_ok, violations = _validate_sql_identifiers(es, id_allowlist)
            if not sql_ok:
                logger.warning(
                    "Rejecting add_example_sql with hallucinated identifiers: %s — %.120s",
                    violations, es,
                )
                continue

            if spark is not None:
                try:
                    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql
                    is_valid, err = validate_ground_truth_sql(
                        es, spark, catalog=catalog, gold_schema=gold_schema,
                        execute=True,
                        parameters=p.get("parameters"),
                        w=w, warehouse_id=warehouse_id,
                    )
                    if not is_valid:
                        logger.warning(
                            "Rejecting add_example_sql that failed validation: %s — %.120s",
                            err, es,
                        )
                        continue
                except Exception:
                    logger.debug("Example SQL execution validation skipped (error)", exc_info=True)

            added_slots += 1

        # Bug #4 firewall — catches near-verbatim copies that slipped past
        # the per-field duplicate checks. Runs last so cheap validators reject
        # first; only non-duplicate, well-shaped proposals reach here.
        if bug4_corpus is not None:
            from genie_space_optimizer.optimization.leakage import is_benchmark_leak
            is_leak, leak_reason = is_benchmark_leak(p, ptype, bug4_corpus)
            if is_leak:
                _incr_bug4_counter("firewall_rejections")
                logger.info(
                    "Bug #4 firewall: Lever 5 %s rejected - %s",
                    ptype, leak_reason,
                )
                continue

        valid.append(p)

    rejected = len(proposals) - len(valid)
    if rejected:
        logger.info(
            "Lever 5 proposal validation: %d rejected, %d kept", rejected, len(valid)
        )
    return valid


# ── Plan 2 / Task 8 — defense-in-depth no-SQL gate for L5a ────────────

_FENCED_SQL_RE = re.compile(r"```sql\b", re.IGNORECASE)
# Note: plan-listed regex included ``\Z`` in the terminator alternation,
# but that made it fire on prose like "Select the right fact table from
# these options." (no WHERE/GROUP/;). Dropping ``\Z`` requires a real
# SQL terminator, which is what the plan's prose-tolerant intent
# specifies (see test_validate_5a_allows_short_select_mentions_in_prose).
_SELECT_FROM_RE = re.compile(
    r"\bSELECT\b[\s\S]{1,200}?\bFROM\b[\s\S]{0,200}?(?:\bWHERE\b|\bGROUP\b|;)",
    re.IGNORECASE,
)


def _validate_lever_5a_no_sql_output(result: dict) -> tuple[bool, str]:
    """Defense-in-depth gate: reject any L5a output that contains SQL.

    The output schema in ``LEVER_5A_INSTRUCTION_PROMPT`` already
    forbids ``example_sql_proposals`` and SQL blocks. This validator
    catches the LLM going off-script AND defends against post-LLM
    code-path bugs that might re-introduce the forbidden key.

    Returns ``(ok, reason)``. ``ok=True`` means the result is publishable.

    Detectors (in order, first match wins):
      1. Forbidden top-level key ``example_sql_proposals`` in the dict.
         **Sentinel post commit 70968193**: with ``strict: true``
         typed-IO + ``additionalProperties: false`` enforced server-
         side, the LLM cannot emit this key. This detector now
         catches code-path bugs where someone tries to inject the
         field post-LLM. Monitor the rate via the
         ``validate_no_sql_result`` MLflow span tag — any
         "rejected:forbidden_key" hit is a code bug, not an LLM issue.
      2. Fenced ```sql code block in ``instruction_text`` (case-insensitive).
      3. ``SELECT ... FROM ...`` pattern of >=40 chars in ``instruction_text``
         that resembles an actual query (heuristic; tuned to avoid prose
         like "select the right table").
    """
    if not isinstance(result, dict):
        return False, "result is not a dict"
    if "example_sql_proposals" in result:
        return False, (
            "L5a output contains forbidden top-level key 'example_sql_proposals'; "
            "use lever-5b-example-sql for SQL proposals."
        )
    instruction_text = result.get("instruction_text") or ""
    if not isinstance(instruction_text, str):
        return False, "instruction_text is not a string"
    if not instruction_text.strip():
        # Empty is fine — L5a's way to say "no changes this iteration".
        return True, ""
    if _FENCED_SQL_RE.search(instruction_text):
        return False, (
            "L5a instruction_text contains a fenced SQL block (```sql); "
            "instructions are prose only, not SQL."
        )
    m = _SELECT_FROM_RE.search(instruction_text)
    if m and len(m.group(0)) >= 40:
        return False, (
            f"L5a instruction_text contains a SELECT...FROM... pattern "
            f"({len(m.group(0))} chars) that resembles SQL: {m.group(0)[:80]!r}"
        )
    return True, ""


def _DEPRECATED_mine_benchmark_example_sqls_verbatim(
    benchmarks: list[dict],
    metadata_snapshot: dict,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
) -> list[dict]:
    """DEPRECATED — disabled as part of Bug #4 (benchmark leakage) remediation.

    This function used to copy benchmark ``expected_sql`` into ``example_sqls``
    verbatim, which contaminates the training signal: the optimizer was then
    evaluated on benchmarks whose exact SQL it had just installed into the
    space. Structural synthesis (see ``_synthesize_example_sqls``) replaces
    this path.

    Gated behind ``GSO_ALLOW_VERBATIM_MINING=1`` for emergency rollback only.
    Without the flag this function raises ``RuntimeError``.
    """
    import os as _os
    if _os.getenv("GSO_ALLOW_VERBATIM_MINING", "0").lower() not in {"1", "true", "yes", "on"}:
        raise RuntimeError(
            "_DEPRECATED_mine_benchmark_example_sqls_verbatim is disabled "
            "(benchmark leakage prevention, Bug #4). Use structural synthesis "
            "via _synthesize_example_sqls instead. Set "
            "GSO_ALLOW_VERBATIM_MINING=1 only for emergency rollback."
        )

    from genie_space_optimizer.common.genie_schema import count_instruction_slots, MAX_INSTRUCTION_SLOTS

    current_slots = count_instruction_slots(metadata_snapshot)
    remaining_budget = max(0, MAX_INSTRUCTION_SLOTS - current_slots)

    if remaining_budget <= 0:
        logger.info(
            "Benchmark mining: skipping — slot budget already exhausted (%d/%d)",
            current_slots, MAX_INSTRUCTION_SLOTS,
        )
        print(
            f"\n-- BENCHMARK EXAMPLE SQL MINING {'─' * 40}\n"
            f"  |  Slot budget exhausted:      {current_slots}/{MAX_INSTRUCTION_SLOTS} — skipping\n"
            + "─" * 72
        )
        return []

    existing_eqs_raw = _get_existing_example_sqls(metadata_snapshot)
    existing_questions: set[str] = set()
    for e in existing_eqs_raw:
        if isinstance(e, dict):
            q = e.get("question", "")
            if isinstance(q, list):
                q = " ".join(q)
            q = q.lower().strip()
            if q:
                existing_questions.add(q)

    proposals: list[dict] = []
    skipped_no_sql = 0
    skipped_dup = 0
    skipped_invalid = 0

    for b in benchmarks:
        sql = (b.get("expected_sql") or "").strip()
        question = (b.get("question") or "").strip()
        if not sql or not question:
            skipped_no_sql += 1
            continue

        q_norm = question.lower().strip()
        if q_norm in existing_questions or any(
            _ngram_similarity(q_norm, eq) > _EXAMPLE_SQL_SIMILARITY_THRESHOLD
            for eq in existing_questions
        ):
            skipped_dup += 1
            continue
        existing_questions.add(q_norm)

        if spark is not None:
            try:
                from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql
                is_valid, err = validate_ground_truth_sql(
                    sql, spark, catalog=catalog, gold_schema=gold_schema,
                    execute=True,
                    parameters=b.get("parameters"),
                    w=w, warehouse_id=warehouse_id,
                )
                if not is_valid:
                    logger.info(
                        "Benchmark mining: skipping invalid SQL for '%.60s': %s",
                        question, err,
                    )
                    skipped_invalid += 1
                    if skipped_invalid <= 3:
                        print(f"  |    [skip] '{question[:50]}': {err[:120]}")
                    continue
                try:
                    test_rows = spark.sql(f"SELECT * FROM ({sql}) LIMIT 1").collect()
                    if not test_rows:
                        logger.info(
                            "Benchmark mining: skipping 0-row result for '%.60s'",
                            question,
                        )
                        skipped_invalid += 1
                        continue
                except Exception:
                    pass
            except Exception as _val_exc:
                logger.debug("Benchmark mining: validation error, skipping", exc_info=True)
                skipped_invalid += 1
                if skipped_invalid <= 3:
                    print(f"  |    [skip] '{question[:50]}': {type(_val_exc).__name__}: {str(_val_exc)[:100]}")
                continue

        if len(proposals) >= remaining_budget:
            logger.info(
                "Benchmark mining: stopping — slot budget exhausted (%d/%d used)",
                current_slots + len(proposals), MAX_INSTRUCTION_SLOTS,
            )
            break

        proposals.append({
            "proposal_id": f"P_BM_{len(proposals) + 1:03d}",
            "cluster_id": f"BENCHMARK_EX_{len(proposals) + 1:03d}",
            "lever": 5,
            "scope": "genie_config",
            "patch_type": "add_example_sql",
            "change_description": f"Benchmark-mined example SQL: {question[:80]}",
            "proposed_value": question,
            "example_question": question,
            "example_sql": sql,
            "parameters": b.get("parameters", []) or [],
            "usage_guidance": f"Mined from benchmark ground truth for: {question[:100]}",
            "rationale": "Pre-validated benchmark SQL mined as example SQL",
            "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
            "confidence": 0.95,
            "questions_fixed": 1,
            "questions_at_risk": 0,
            "net_impact": 0.95,
            "asi": {
                "failure_type": "asset_routing_error",
                "blame_set": [],
                "severity": "minor",
                "counterfactual_fixes": [],
                "ambiguity_detected": False,
            },
            "provenance": {
                "cluster_id": f"BENCHMARK_EX_{len(proposals):03d}",
                "root_cause": "benchmark_mining",
                "originating_questions": [question],
                "lever": 5,
                "lever_name": "Benchmark Example SQL Mining",
                "patch_type": "add_example_sql",
            },
        })

    logger.info(
        "Benchmark mining: %d proposals, %d skipped (no sql=%d, dup=%d, invalid=%d)",
        len(proposals), skipped_no_sql + skipped_dup + skipped_invalid,
        skipped_no_sql, skipped_dup, skipped_invalid,
    )
    print(
        f"\n-- BENCHMARK EXAMPLE SQL MINING {'─' * 40}\n"
        f"  |  Benchmarks scanned:         {len(benchmarks)}\n"
        f"  |  Slot budget:                {current_slots}/{MAX_INSTRUCTION_SLOTS} "
        f"(remaining: {remaining_budget})\n"
        f"  |  Skipped (no SQL):           {skipped_no_sql}\n"
        f"  |  Skipped (duplicate):        {skipped_dup}\n"
        f"  |  Skipped (invalid SQL):      {skipped_invalid}\n"
        f"  |  Proposals generated:        {len(proposals)}\n"
        + "─" * 72
    )
    return proposals


def _ngram_similarity(a: str, b: str, n: int = 3) -> float:
    """Compute Jaccard similarity over character n-grams."""
    if not a or not b:
        return 0.0
    a_lower, b_lower = a.lower(), b.lower()
    a_ngrams = {a_lower[i : i + n] for i in range(len(a_lower) - n + 1)}
    b_ngrams = {b_lower[i : i + n] for i in range(len(b_lower) - n + 1)}
    if not a_ngrams or not b_ngrams:
        return 0.0
    return len(a_ngrams & b_ngrams) / len(a_ngrams | b_ngrams)


# ── Lever 6: SQL Expressions ──────────────────────────────────────────


def _format_existing_sql_snippets(metadata_snapshot: dict) -> str:
    """Format existing SQL snippets for the Lever 6 prompt context.

    Each row includes the source table parsed from the snippet's SQL
    so the LLM has explicit disambiguation context. Without this hint
    the model can produce generic names like ``Month-to-Date Filter``
    even when the space already has one for a different fact table.
    """
    snippets = metadata_snapshot.get("sql_snippets", {})
    if not isinstance(snippets, dict):
        return "(No existing SQL expressions.)"

    lines: list[str] = []
    for snippet_type in ("measures", "filters", "expressions"):
        items = snippets.get(snippet_type, []) or []
        if not items:
            continue
        lines.append(f"\n### {snippet_type.title()}")
        for item in items:
            name = item.get("display_name", item.get("alias", "unnamed"))
            sql = item.get("sql", [])
            sql_str = sql[0] if isinstance(sql, list) and sql else str(sql)
            syns = item.get("synonyms", [])
            primary_table = _extract_primary_table_identifier(sql_str)
            short_table = (
                primary_table.rsplit(".", 1)[-1] if primary_table else ""
            )
            tag = f" [{short_table}]" if short_table else ""
            lines.append(f"  - {name}{tag}: `{sql_str}`")
            if syns:
                lines.append(f"    Synonyms: {', '.join(syns)}")

    return "\n".join(lines) if lines else "(No existing SQL expressions.)"


# G1 (2026-05-17 lever-6 hardening plan, Task 3) — failure_type → snippet_type
# routing prior. See docs/prompt_improvements/2026-05-17-lever6-empirical-baseline.md
# for the empirical distribution that motivated each pairing.
_LEVER_6_FAILURE_TYPE_ROUTING: tuple[tuple[str, str, str], ...] = (
    # (failure_type, preferred_snippet_type, rationale)
    ("plural_top_n_collapse",     "expression",
     "Top-N defects (RANK ties expanding the result set) need a ROW_NUMBER()/window expression"),
    ("missing_filter",            "filter",
     "A boolean WHERE clause is missing — emit a reusable filter snippet"),
    ("wrong_filter_condition",    "filter",
     "An existing filter has the wrong shape — emit the corrected boolean expression"),
    ("wrong_aggregation",         "measure",
     "Aggregation function is wrong (e.g. SUM vs COUNT) — emit a measure with the correct aggregate"),
    ("missing_dimension",         "expression",
     "Derived per-row column (e.g. MONTH(date), CASE bucket) is missing — emit an expression"),
    ("currency_or_unit_mismatch", "measure",
     "Result is in the wrong unit/currency — emit a unit-aware aggregation measure"),
)


def _render_failure_type_to_snippet_type_table() -> str:
    """Render the typed failure_type → snippet_type routing prior for the
    lever-6 prompt.

    The table is the strongest deterministic prior available for the typed
    failure-types observed in Trial-5 (67% of inputs). The other 33% (free-
    form strategist prose) falls through to LLM choice — explicitly
    documented as an escape hatch.
    """
    header = (
        "When the cluster's failure_type matches one of the typed values "
        "below, prefer the indicated snippet_type unless the cluster "
        "evidence clearly points elsewhere:"
    )
    rows = ["| failure_type | snippet_type | rationale |", "|---|---|---|"]
    for failure, snippet, rationale in _LEVER_6_FAILURE_TYPE_ROUTING:
        rows.append(f"| `{failure}` | `{snippet}` | {rationale} |")
    rows.append("")
    rows.append(
        "When the failure_type is free-form prose (an adaptive strategist "
        "describing the failure in natural language), no deterministic "
        "prior applies — use the cluster's `blame_set`, `counterfactual_fixes`, "
        "and `structural_diff` to choose the snippet_type yourself."
    )
    return header + "\n\n" + "\n".join(rows)


def _filter_rca_synonyms(
    candidates: list[Any], existing: list[str],
) -> list[str]:
    """Drop low-quality synonym candidates and dedupe against existing list."""
    existing_lower = {str(s).strip().lower() for s in existing}
    out: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        s = str(raw).strip().lower()
        if len(s) < 2:
            continue
        if s in existing_lower or s in seen:
            continue
        # Drop SQL-shaped tokens (snake_case without spaces)
        if "_" in s and " " not in s:
            continue
        if s.isupper():
            continue
        out.append(s)
        seen.add(s)
        if len(out) >= 5:
            break
    return out


def _render_rca_bridge_slots(
    *,
    is_table_level: bool,
    table: str,
    column: str,
    afs_projections: list[dict],
    expected_objects: list[str],
    actual_objects: list[str],
    existing_synonyms: list[str],
) -> dict[str, Any]:
    """Pre-render RCA-bridge prompt slots as clean strings.

    The SKILL.md template uses simple {{ name }} slots; this helper does
    the shape adaptation (Python list -> joined string, JSON list ->
    structured markdown) so the template stays declarative and the model
    never sees Python repr artifacts.

    Plan: 2026-05-17-lever-1-rca-bridge-hardening.md Task 4
    """
    import json as _json

    target_label = (
        f"table {table}" if is_table_level else f"column {table}.{column}"
    )

    expected_joined = ", ".join(expected_objects) if expected_objects else "(none)"
    actual_joined = ", ".join(actual_objects) if actual_objects else "(none)"

    if not afs_projections:
        afs_rendered = "(no failure clusters)"
    else:
        lines: list[str] = []
        for i, proj in enumerate(afs_projections, start=1):
            failure_type = str(proj.get("failure_type", "(unknown)"))
            blame_list = proj.get("blame_set", []) or []
            blame_joined = ", ".join(str(b) for b in blame_list) if blame_list else "(none)"
            structural_diff = proj.get("structural_diff", {}) or {}
            lines.append(f"### Cluster {i}")
            lines.append(f"- failure_type: {failure_type}")
            lines.append(f"- blame_set: {blame_joined}")
            if structural_diff:
                lines.append(
                    f"- structural_diff: {_json.dumps(structural_diff, default=str)}"
                )
            lines.append("")
        afs_rendered = "\n".join(lines).rstrip()

    if existing_synonyms:
        existing_syn_rendered = "\n".join(f"- {s}" for s in existing_synonyms)
    else:
        existing_syn_rendered = "(none)"

    if is_table_level:
        synonyms_instruction_rule = ""
        output_schema_block = (
            'Table-level output (NO synonyms key):\n'
            '{"description": "<1-3 sentences, max 300 chars>"}'
        )
    else:
        synonyms_instruction_rule = (
            "- **Synonyms must NOT be SQL identifiers.** No snake_case "
            "(containing `_`), no ALL_CAPS, no single-character entries."
        )
        output_schema_block = (
            'Column-level output:\n'
            '{"description": "<1-3 sentences, max 300 chars>",\n'
            ' "synonyms": ["<lowercase NL phrase 1>", "<lowercase NL phrase 2>", "..."]}'
        )

    return {
        "target_label": target_label,
        "is_table_level": is_table_level,
        "expected_objects_joined": expected_joined,
        "actual_objects_joined": actual_joined,
        "afs_projections_rendered": afs_rendered,
        "existing_synonyms_rendered": existing_syn_rendered,
        "synonyms_instruction_rule": synonyms_instruction_rule,
        "output_schema_block": output_schema_block,
    }


def _generate_lever1_rca_proposal(
    theme: Any,
    patch: dict,
    metadata_snapshot: dict,
    *,
    w: "WorkspaceClient | None" = None,
    benchmarks: list[dict] | None = None,
) -> dict | None:
    """Generate a Lever-1 column/table proposal from an RCA theme patch.

    Calls the LLM to produce a description and (for column patches) a
    synonyms list. Uses the AFS projection of source clusters for
    leakage safety: only sanitized failure_type, blame_set, and the
    target_qids' question phrases reach the LLM.
    """
    import json as _json
    from genie_space_optimizer.optimization.afs import format_afs
    from genie_space_optimizer.optimization.leakage import is_benchmark_leak

    ptype = str(patch.get("type") or "")
    intent = str(patch.get("intent") or "").strip()
    rca_id = str(getattr(theme, "rca_id", ""))
    target_qids = list(getattr(theme, "target_qids", ()) or ())

    if ptype == "update_description":
        target = str(patch.get("target") or "")
        if not target:
            return None
        is_table_level = True
        table, column = target, ""
    else:
        table = str(patch.get("table") or "")
        column = str(patch.get("column") or "")
        if not column:
            return None
        is_table_level = False
        # Producer-side strict shape contract (Task 5). Reject malformed
        # column targets at the producer so they never burn cap budget
        # downstream, and so any future regression is visible in logs.
        from genie_space_optimizer.optimization.proposal_shape import (
            ProposalShapeError,
            validate_column_proposal_shape,
        )
        try:
            validate_column_proposal_shape({
                "proposal_id": rca_id or "rca_theme",
                "patch_type": ptype,
                "table": table,
                "column": column,
            })
        except ProposalShapeError:
            logger.info(
                "Rejected malformed Lever-1 RCA target rca_id=%s patch_type=%s "
                "table=%r column=%r",
                rca_id, ptype, table, column,
                exc_info=True,
            )
            return None

    failure_clusters = (
        metadata_snapshot.get("_failure_clusters")
        or metadata_snapshot.get("failure_clusters")
        or []
    )
    qid_set = set(target_qids)
    relevant_clusters = [
        c for c in failure_clusters
        if isinstance(c, dict)
        and qid_set & set(c.get("question_ids", []) or [])
    ]
    afs_projections = [format_afs(c) for c in relevant_clusters[:3]]

    expected_objects = list(patch.get("expected_objects") or [])
    actual_objects = list(patch.get("actual_objects") or [])

    existing_synonyms: list[str] = []
    existing_description = ""
    tables = metadata_snapshot.get("tables") or []
    for t in tables:
        if not isinstance(t, dict):
            continue
        if t.get("identifier") == table or t.get("name") == table:
            if is_table_level:
                existing_description = str(t.get("description", "") or "")
            else:
                for col in t.get("columns") or []:
                    if isinstance(col, dict) and col.get("name") == column:
                        existing_description = str(col.get("description", "") or "")
                        existing_synonyms = list(col.get("synonyms") or [])
                        break

    # Plan 2026-05-17-lever-1-rca-bridge-hardening Tasks 3, 5, 6, 7, 8 —
    # drop existing_description[:300] truncation; route slot-rendering
    # through _render_rca_bridge_slots; pass max_tokens, response_model,
    # and the domain-framed system message.
    from genie_space_optimizer.common.config import (
        LEVER_1_2_SYSTEM_MSG,
        LEVER_1_RCA_BRIDGE_MAX_TOKENS,
        LEVER_1_RCA_BRIDGE_PROMPT,
    )
    from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
    from genie_space_optimizer.optimization.prompt_io import Lever1RcaBridgeOutput

    format_kwargs = {
        **_render_rca_bridge_slots(
            is_table_level=is_table_level,
            table=table,
            column=column,
            afs_projections=afs_projections,
            expected_objects=expected_objects,
            actual_objects=actual_objects,
            existing_synonyms=existing_synonyms,
        ),
        "intent": intent,
        "existing_description": existing_description,
    }
    prompt = format_mlflow_template(LEVER_1_RCA_BRIDGE_PROMPT, **format_kwargs)
    _link_prompt_to_trace("lever_1_rca_bridge")

    try:
        raw_text, _ = _traced_llm_call(
            w, LEVER_1_2_SYSTEM_MSG, prompt,
            span_name="lever1_rca_proposal",
            max_tokens=LEVER_1_RCA_BRIDGE_MAX_TOKENS,
            response_model=Lever1RcaBridgeOutput,
        )
    except Exception:
        logger.warning(
            "Lever-1 RCA bridge LLM call failed for %s", rca_id, exc_info=True,
        )
        return None

    from genie_space_optimizer.optimization.evaluation import _extract_json
    parsed = _extract_json(raw_text)
    if not isinstance(parsed, dict):
        return None
    description = str(parsed.get("description") or "").strip()
    synonyms_raw = parsed.get("synonyms") or []
    if not isinstance(synonyms_raw, list):
        synonyms_raw = []
    synonyms = _filter_rca_synonyms(synonyms_raw, existing_synonyms)

    if benchmarks:
        is_leak, _reason = is_benchmark_leak(
            {"description": description, "patch_type": ptype},
            ptype, benchmarks,
        )
        if is_leak:
            logger.info("Lever-1 RCA proposal rejected (leakage)")
            return None

    if not description and not synonyms:
        return None

    if is_table_level:
        return {
            "patch_type": "update_description",
            "table": table,
            "table_sections": {"description": description} if description else {},
            "table_entity_type": "table",
        }
    sections: dict[str, Any] = {}
    if description:
        sections["description"] = description
    if synonyms:
        sections["synonyms"] = synonyms
    return {
        "patch_type": ptype,
        "table": table,
        "column": column,
        "column_sections": sections,
        "column_entity_type": "",
        "_rca_synonyms": synonyms,
    }


_STRUCTURAL_SQL_SNIPPET_PATCH_TYPES = {
    "measure": "add_sql_snippet_measure",
    "filter": "add_sql_snippet_filter",
    "expression": "add_sql_snippet_expression",
}


def _proposal_from_structural_sql_candidate(
    candidate: dict,
    *,
    metadata_snapshot: dict,
    cluster_id: str,
    target_qids: tuple[str, ...],
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: WorkspaceClient | None = None,
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
) -> dict | None:
    """Translate one structural SQL candidate into a Lever 6 proposal.

    Refuses to emit anything other than ``add_sql_snippet_*`` patch types
    when the source tag is ``rca_failed_question_sql``. This is the
    primary firewall preventing failed-row benchmark SQL from leaking
    into Example SQL artifacts.
    """
    del benchmarks  # Reserved for future post-validation hooks.
    if not isinstance(candidate, dict):
        return None
    if candidate.get("source") != "rca_failed_question_sql":
        return None
    snippet_type = str(candidate.get("snippet_type") or "").strip().lower()
    patch_type = _STRUCTURAL_SQL_SNIPPET_PATCH_TYPES.get(snippet_type)
    if patch_type is None:
        logger.warning(
            "Rejected rca_failed_question_sql candidate with non-snippet type=%s",
            snippet_type,
        )
        return None
    sql_raw = str(candidate.get("sql") or "").strip()
    if not sql_raw:
        return None

    sql_ok, violations = _validate_sql_identifiers(
        sql_raw,
        _build_identifier_allowlist(metadata_snapshot),
    )
    if not sql_ok:
        logger.info(
            "Lever 6 structural candidate rejected by identifier allowlist: %s",
            violations,
        )
        return None

    validation_passed = False
    if spark is not None or (w is not None and warehouse_id):
        from genie_space_optimizer.optimization.benchmarks import validate_sql_snippet

        valid_result = validate_sql_snippet(
            sql_raw,
            snippet_type,
            metadata_snapshot,
            spark=spark,
            catalog=catalog,
            gold_schema=gold_schema,
            w=w,
            warehouse_id=warehouse_id,
        )
        if not valid_result[0]:
            logger.info(
                "Lever 6 structural candidate validation failed kind=%s reason=%s",
                snippet_type,
                valid_result[1],
            )
            return None
        sql_raw = valid_result[2] if len(valid_result) > 2 else sql_raw
        validation_passed = True

    _qualified = _qualify_sql_snippet_metadata(
        {
            "snippet_type": snippet_type,
            "sql": sql_raw,
            "display_name": candidate.get("display_name", ""),
            "instruction": candidate.get("instruction", ""),
            "target_table": candidate.get("target_table", ""),
        },
        target_table=str(candidate.get("target_table", "") or ""),
    )

    return {
        "patch_type": patch_type,
        "lever": 6,
        "snippet_type": snippet_type,
        "display_name": _qualified.get("display_name", ""),
        "alias": candidate.get("alias", ""),
        "sql": sql_raw,
        "synonyms": candidate.get("synonyms", []),
        "instruction": _qualified.get("instruction", ""),
        "target_table": candidate.get("target_table", ""),
        "rationale": candidate.get("evidence", "RCA structural SQL learning"),
        "affected_questions": list(target_qids),
        "target_qids": list(target_qids),
        "confidence": float(candidate.get("confidence", 0.85) or 0.85),
        "questions_fixed": len(target_qids),
        "validation_passed": validation_passed,
        "source": "rca_failed_question_sql",
        "source_question_id": candidate.get("source_question_id", ""),
        "cluster_id": cluster_id,
    }


def _l6_dict_to_repair_proposal(
    candidate: dict,
    *,
    intent_id: str,
    rca_card_id: str,
    causal_target: str,
    repair_hypothesis: str,
) -> Any:
    """Plan 12 — bridge the legacy L6 candidate dict (output of
    :func:`_proposal_from_structural_sql_candidate`) into a typed
    :class:`RepairProposal` that satisfies the survival contract.

    Returns ``None`` if the candidate cannot be promoted (missing
    ``target_table``, empty ``target_qids``, or unknown ``patch_type``).
    """
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType,
        RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    from genie_space_optimizer.optimization.target_object_typed import (
        AssetKind,
        TargetObject,
    )

    target_table = str(candidate.get("target_table") or "").strip()
    target_qids = tuple(
        str(q)
        for q in (
            candidate.get("target_qids")
            or candidate.get("affected_questions")
            or []
        )
        if str(q)
    )
    if not target_table or not target_qids:
        return None

    try:
        ptype = PatchType(str(candidate.get("patch_type") or ""))
    except ValueError:
        return None

    target_object = TargetObject(
        asset_kind=AssetKind.TABLE,
        identifier=target_table,
        columns=(causal_target,) if causal_target else (),
    )

    if causal_target:
        blame_set: tuple[str, ...] = (causal_target,)
    elif target_table:
        blame_set = (target_table,)
    else:
        blame_set = ()

    raw_conf = float(candidate.get("confidence", 0.85) or 0.0)
    confidence: str
    if raw_conf >= 0.8:
        confidence = "high"
    elif raw_conf >= 0.5:
        confidence = "medium"
    else:
        confidence = "low"

    return RepairProposal(
        intent_id=str(intent_id),
        intent_name=str(candidate.get("display_name") or "L6 SQL snippet"),
        intent_description=str(
            candidate.get("instruction") or candidate.get("rationale") or ""
        ),
        repair_shape=RepairShape.OTHER,
        patch_type=ptype,
        rationale=str(candidate.get("rationale") or ""),
        confidence=confidence,  # type: ignore[arg-type]
        patch_body={
            "name": str(candidate.get("display_name") or "l6_snippet"),
            "sql_expression": str(candidate.get("sql") or ""),
            "usage_guidance": str(candidate.get("instruction") or ""),
        },
        blame_set=blame_set,
        target_objects=(target_object,),
        required_constructs=(),
        repair_hypothesis=str(repair_hypothesis or ""),
        target_qids=target_qids,
    )


def _generate_proposals_for_lever6(
    *,
    action_group: dict,
    metadata_snapshot: dict,
    ag_id: str,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    w: Any = None,
    benchmarks: list[dict] | None = None,
) -> list[dict]:
    """Plan 12 — extracted L6 proposal generator with full
    survival-contract threading.

    For each structural SQL candidate emitted upstream by
    ``rca_failed_question_sql``:

      1. Run the legacy dict-shaped converter
         :func:`_proposal_from_structural_sql_candidate`.
      2. Look up ``rca_card_id`` / ``causal_target`` / ``repair_hypothesis``
         from the cluster's ``_failure_clusters`` entry (by
         ``source_cluster_ids`` membership).
      3. Stamp those fields plus ``original_patch_type`` onto the
         proposal dict so downstream consumers (narrow_replacement,
         structural_repair_gate, applier) have them.
      4. Return the list of proposal dicts. The Stage-3 callsite
         continues to set ``proposal_id`` / ``scope`` / ``provenance``
         on each entry.
    """
    target_qids = tuple(
        str(q)
        for q in (action_group.get("affected_questions") or [])
        if str(q)
    )
    structural_candidates = [
        c
        for c in (action_group.get("_lever6_structural_candidates") or [])
        if isinstance(c, dict)
    ]

    source_cids = set(action_group.get("source_cluster_ids", []))
    all_clusters = (
        metadata_snapshot.get("_failure_clusters")
        or metadata_snapshot.get("failure_clusters")
        or []
    )
    eligible_clusters = [
        c for c in all_clusters if c.get("cluster_id") in source_cids
    ]
    primary_cluster = eligible_clusters[0] if eligible_clusters else {}

    rca_card_id = str(primary_cluster.get("rca_card_id") or "")
    causal_target = str(primary_cluster.get("causal_target") or "")
    repair_hypothesis = str(primary_cluster.get("repair_hypothesis") or "")

    optimization_run_id_l6 = str(
        metadata_snapshot.get("optimization_run_id") or ""
    )
    iteration_l6 = int(metadata_snapshot.get("iteration") or 0)
    cluster_id_l6 = str(primary_cluster.get("cluster_id") or "")

    proposals: list[dict] = []
    for candidate in structural_candidates:
        proposal = _proposal_from_structural_sql_candidate(
            candidate,
            metadata_snapshot=metadata_snapshot,
            cluster_id=ag_id,
            target_qids=target_qids,
            spark=spark,
            catalog=catalog,
            gold_schema=gold_schema,
            w=w,
            warehouse_id=warehouse_id,
            benchmarks=benchmarks,
        )
        if not proposal:
            # Plan 12 — candidate failed to promote through the legacy
            # converter (identifier-allowlist firewall, SQL validation,
            # missing target_table, or unknown snippet_type). Emit a
            # CONTRACT_FAILED terminal outcome so I22 sees what
            # happened. Without this, the L6 lane silently drops
            # the candidate and the postmortem has no per-intent record.
            from genie_space_optimizer.optimization.patch_outcome import (
                PatchOutcomeKind,
            )
            from genie_space_optimizer.optimization.patch_survival_emitter import (
                emit_patch_outcome,
            )
            intent_id_l6 = (
                f"l6_{ag_id}_"
                f"{candidate.get('source_question_id') or 'unknown'}"
            )
            emit_patch_outcome(
                optimization_run_id=optimization_run_id_l6,
                iteration=iteration_l6,
                ag_id=str(ag_id),
                cluster_id=cluster_id_l6,
                intent_id=intent_id_l6,
                outcome_kind=PatchOutcomeKind.CONTRACT_FAILED,
                terminal_reason=(
                    "l6_structural_candidate_failed_pre_repair_proposal"
                ),
            )
            continue
        # Plan 12 — survival-contract threading.
        proposal["rca_card_id"] = rca_card_id
        proposal["causal_target"] = causal_target
        proposal["repair_hypothesis"] = repair_hypothesis
        proposal["original_patch_type"] = str(
            proposal.get("patch_type") or ""
        )
        proposal["target_qids"] = list(target_qids)
        proposals.append(proposal)

    return proposals


def _lever6_reject_payload(
    *,
    reason: str,
    cluster_id: str,
    target_table: str = "",
    detail: Any = "",
) -> dict[str, Any]:
    """Structured payload for Lever-6 rejection MLflow spans.

    Reason values are intentionally a closed enum so trace dashboards can
    group rejection causes:
      - ``llm_call_failed``
      - ``unparseable_json``
      - ``invalid_snippet_type``
      - ``empty_sql``
      - ``invalid_identifiers``
      - ``snippet_validation_failed``
    """
    return {
        "rejected": True,
        "reject_reason": reason,
        "cluster_id": str(cluster_id or ""),
        "target_table": str(target_table or ""),
        "detail": detail,
    }


_LEVER3_ROOT_CAUSES = frozenset({
    "missing_data_asset",
    "wrong_function",
    "incorrect_function_usage",
    "tvf_parameter_error",
})


def _cluster_expects_lever3(cluster: dict[str, Any]) -> bool:
    """Return True when a cluster looks like it should route to Lever 3.

    Either the root cause is a known Lever-3 family, or the blame set names
    a SQL routine (``fn_*`` or anything containing ``function``).
    """
    root_cause = str(cluster.get("root_cause") or "").strip().lower()
    if root_cause in _LEVER3_ROOT_CAUSES:
        return True
    blame = cluster.get("asi_blame_set") or cluster.get("blame_set") or []
    if isinstance(blame, str):
        blame_items = [blame]
    else:
        blame_items = [str(item) for item in blame]
    return any("fn_" in item.lower() or "function" in item.lower() for item in blame_items)


def _strategist_memo_key(
    clusters: list[dict[str, Any]],
    metadata_snapshot: dict[str, Any],
    *,
    sql_shape_deltas: list[dict[str, Any]] | None = None,
) -> str:
    """Deterministic key for memoizing adaptive strategist results.

    Same cluster signatures + same space revision produce the same key, so
    repeated iterations against unchanged failure clusters can short-circuit
    the strategist call.

    v2 Task 23 — once Tasks 19/20 land, rejected AGs carry
    ``sql_shape_deltas``. Including a fingerprint of those deltas in the
    key prevents the strategist memo cache from returning a stale
    strategy after a rollback whose only signal was a SQL-shape change.
    """
    cluster_parts: list[str] = []
    for cluster in clusters:
        sig = str(cluster.get("cluster_signature") or cluster.get("cluster_id") or "")
        root = str(cluster.get("root_cause") or "")
        qids = ",".join(sorted(str(q) for q in (cluster.get("question_ids") or [])))
        blame = cluster.get("asi_blame_set") or cluster.get("blame_set") or []
        if isinstance(blame, str):
            blame_s = blame
        else:
            blame_s = ",".join(sorted(str(b) for b in blame))
        cluster_parts.append(f"{sig}:{root}:{qids}:{blame_s}")
    revision = str(
        metadata_snapshot.get("space_revision")
        or metadata_snapshot.get("config_version")
        or metadata_snapshot.get("space_id")
        or metadata_snapshot.get("revision")
        or ""
    )
    delta_parts: list[str] = []
    for delta in sql_shape_deltas or []:
        target = str(delta.get("target_qid") or "")
        improved = ",".join(sorted(str(x) for x in (delta.get("improved") or [])))
        remaining = ",".join(sorted(str(x) for x in (delta.get("remaining") or [])))
        delta_parts.append(f"{target}:{improved}:{remaining}")
    raw = (
        "|".join(sorted(cluster_parts))
        + f"|revision={revision}"
        + "|deltas="
        + "|".join(sorted(delta_parts))
    )
    return raw[:2000]


def _diagnose_lever3_directive_emission(
    clusters: list[dict[str, Any]],
    strategy: dict[str, Any],
) -> list[dict[str, Any]]:
    """Report clusters that expect Lever 3 but lack a strategist action group.

    Used after adaptive strategy generation so a missing Lever-3 directive is
    visible in logs and trace metadata instead of silently dropping the RCA.
    """
    action_groups = strategy.get("action_groups") or []
    diagnostics: list[dict[str, Any]] = []
    for cluster in clusters:
        if not _cluster_expects_lever3(cluster):
            continue
        qids = [str(q) for q in (cluster.get("question_ids") or [])]
        has_l3 = False
        for ag in action_groups:
            lever = int(ag.get("target_lever") or ag.get("lever") or 0)
            affected = {str(q) for q in (ag.get("affected_questions") or [])}
            if lever == 3 and (not qids or affected.intersection(qids)):
                has_l3 = True
                break
        if not has_l3:
            diagnostics.append({
                "cluster_id": str(cluster.get("cluster_id") or ""),
                "expected_lever": 3,
                "status": "missing_lever3_action_group",
                "question_ids": qids,
                "blame_set": cluster.get("asi_blame_set") or cluster.get("blame_set") or [],
            })
    return diagnostics


def _generate_lever6_proposal(
    cluster: dict,
    metadata_snapshot: dict,
    *,
    strategist_hints: list[dict] | None = None,
    w: WorkspaceClient | None = None,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
    raw_evidence: tuple[dict, ...] = (),
    # Plan 8 Task 3 — Plan 5 intent-aware short-circuit kwargs.
    rca_evidence_typed: dict | None = None,
    llm_cluster: Any = None,
    ag_id: str | None = None,
    iteration: int = 0,
    run_id: str | None = None,
) -> dict | None:
    """Generate a SQL Expression proposal for a failure cluster.

    Plan 8 Task 3 — when ``rca_evidence_typed`` + ``llm_cluster`` +
    ``ag_id`` are populated, dispatch through
    ``lever6_intent_dispatch.dispatch_lever_6_with_intent`` which calls
    Plan 5's typed synthesizer first and stamps a typed ``RepairIntent``
    on the returned proposal. ``None`` from the intent dispatch path
    means caller takes the heuristic fallback below.
    """
    if rca_evidence_typed and llm_cluster is not None and ag_id:
        from genie_space_optimizer.optimization.lever6_intent_dispatch import (
            dispatch_lever_6_with_intent,
        )
        out = dispatch_lever_6_with_intent(
            cluster=cluster, metadata_snapshot=metadata_snapshot, w=w,
            rca_evidence_typed=rca_evidence_typed,
            llm_cluster=llm_cluster, ag_id=ag_id, iteration=int(iteration),
            run_id=run_id,
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            warehouse_id=warehouse_id, benchmarks=benchmarks,
            raw_evidence=raw_evidence,
            strategist_hints=strategist_hints,
        )
        if out is not None:
            return out
    return _generate_lever6_proposal_legacy_body(
        cluster=cluster, metadata_snapshot=metadata_snapshot,
        strategist_hints=strategist_hints, w=w, spark=spark,
        catalog=catalog, gold_schema=gold_schema, warehouse_id=warehouse_id,
        benchmarks=benchmarks, raw_evidence=raw_evidence,
    )


def _generate_lever6_proposal_legacy_body(
    cluster: dict,
    metadata_snapshot: dict,
    *,
    strategist_hints: list[dict] | None = None,
    w: WorkspaceClient | None = None,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
    raw_evidence: tuple[dict, ...] = (),
) -> dict | None:
    """Heuristic Lever-6 body — invoked as the fallback when the
    Plan-5 intent-aware short-circuit declines or selects a non-L6
    patch_type. Identical to the pre-Plan-8 body verbatim.

    Calls the LLM with LEVER_6_SQL_EXPRESSION_PROMPT, validates the output
    structurally and by SQL execution, and returns a proposal dict or None
    if validation fails.

    The LLM chooses the snippet_type (measure/filter/expression) — the
    static _LEVER_TO_PATCH_TYPE mapping is NOT used here.

    Bug #4 firewall — when ``benchmarks`` is provided, the proposal is
    checked via ``is_benchmark_leak`` before being returned. Leaky proposals
    are dropped with a counter increment; callers get ``None``.
    """
    import json as _json

    import mlflow
    from mlflow.entities import SpanType as _SpanType

    root_cause = cluster.get("root_cause", "other")

    with mlflow.start_span(name=f"lever6_proposal_{root_cause}", span_type=_SpanType.CHAIN) as span:
        # Bug #4 (P2.2) — Lever 6 cluster context must be the AFS projection.
        # _format_cluster_briefs is retained only for debug/logging.
        from genie_space_optimizer.optimization.afs import format_afs
        _afs_ctx = format_afs(cluster)
        cluster_context = _json.dumps(_afs_ctx, indent=2, default=str)

        # G3 (Task 5) — derive relevant_objects from the cluster's blame_set
        # so the schema dump only carries cluster-relevant tables. Falls back
        # to the full schema when blame_set is empty.
        _blame = _afs_ctx.get("blame_set") or []
        _relevant: set[str] = {str(b).strip() for b in _blame if str(b).strip()}
        schema_context = _format_schema_index(
            metadata_snapshot,
            relevant_objects=_relevant or None,
        )

        # G7 (Task 6) — render the cluster-filtered identifier allowlist so
        # the LLM sees an explicit list of valid identifiers (functions and
        # metric views included, which the schema dump omits). Reuses the
        # Task 5 _relevant set for consistency.
        _allowlist = _build_identifier_allowlist(
            metadata_snapshot,
            relevant_objects=_relevant or None,
        )
        identifier_allowlist = _format_identifier_allowlist(_allowlist)

        existing_snippets = _format_existing_sql_snippets(metadata_snapshot)

        hints_text = "(No strategist hints.)"
        if strategist_hints:
            hints_text = _json.dumps(strategist_hints, indent=2)

        prompt = format_mlflow_template(
            LEVER_6_SQL_EXPRESSION_PROMPT,
            root_cause=root_cause,
            cluster_context=cluster_context,
            schema_context=schema_context,
            existing_sql_snippets=existing_snippets,
            strategist_hints=hints_text,
            raw_evidence_block=_format_raw_evidence_block(raw_evidence),
            failure_type_routing_table=_render_failure_type_to_snippet_type_table(),
            identifier_allowlist=identifier_allowlist,
        )

        span.set_inputs({"root_cause": root_cause, "prompt_chars": len(prompt)})

        # Plan 2026-05-17-prompt-registry-and-typed-io-hygiene Task 2 —
        # link the registered lever_6_sql_expression prompt to the trace.
        from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
        _link_prompt_to_trace("lever_6_sql_expression")

        try:
            from genie_space_optimizer.common.config import LEVER_6_MAX_TOKENS
            from genie_space_optimizer.optimization.prompt_io import (
                Lever6SqlExpressionOutput,
            )
            raw_text, _ = _traced_llm_call(
                w, "You are a SQL expression expert.", prompt,
                span_name="lever6_llm",
                max_tokens=LEVER_6_MAX_TOKENS,
                response_model=Lever6SqlExpressionOutput,
            )
        except Exception:
            logger.warning("Lever 6 LLM call failed for root_cause=%s", root_cause, exc_info=True)
            return None

        from genie_space_optimizer.optimization.evaluation import _extract_json

        llm_result = _extract_json(raw_text)
        if not llm_result or not isinstance(llm_result, dict):
            logger.warning("Lever 6 LLM returned unparseable JSON for root_cause=%s", root_cause)
            return None

        snippet_type = llm_result.get("snippet_type", "")
        if snippet_type not in ("measure", "filter", "expression"):
            logger.warning("Lever 6 LLM returned invalid snippet_type: %s", snippet_type)
            return None

        # G2 (2026-05-17 lever-6 hardening plan, Task 2) — reject proposals
        # whose affected_questions contains IDs outside the cluster's
        # question_ids. Trial-5 baseline: 15% of lever-6 proposals were
        # post-rejected by the applier for this reason. See
        # docs/prompt_improvements/2026-05-17-lever6-empirical-baseline.md
        raw_aq = llm_result.get("affected_questions", []) or []
        aq_set = {str(q).strip() for q in raw_aq if str(q).strip()}
        valid_qids = {
            str(q).strip()
            for q in (cluster.get("question_ids") or [])
            if str(q).strip()
        }
        if aq_set and valid_qids and not aq_set.issubset(valid_qids):
            extra = sorted(aq_set - valid_qids)
            logger.warning(
                "Lever 6 [%s]: affected_questions contains IDs outside cluster.question_ids: %s "
                "(valid: %s) — rejecting proposal",
                cluster.get("cluster_id", "?"), extra, sorted(valid_qids),
            )
            return None

        sql_raw = llm_result.get("sql", "")
        if not sql_raw:
            logger.warning("Lever 6 LLM returned empty sql")
            return None

        sql_ok, violations = _validate_sql_identifiers(
            sql_raw, _build_identifier_allowlist(metadata_snapshot),
        )
        if not sql_ok:
            logger.warning("Lever 6: SQL snippet has invalid identifiers: %s", violations)
            return None

        _target_table_hint = str(llm_result.get("target_table", "") or "").strip()
        _cluster_id_hint = cluster.get("cluster_id", "?")
        if spark is not None or (w is not None and warehouse_id):
            from genie_space_optimizer.optimization.benchmarks import validate_sql_snippet

            _valid_result = validate_sql_snippet(
                sql_raw, snippet_type, metadata_snapshot,
                spark=spark, catalog=catalog, gold_schema=gold_schema,
                w=w, warehouse_id=warehouse_id,
            )
            if not _valid_result[0]:
                # T2.14: every call to validate_sql_snippet now has an
                # outcome log line so runs can grep FAILED vs PASSED.
                logger.info(
                    "Lever 6 [%s]: snippet validation (kind=%s, target=%s): FAILED — %s",
                    _cluster_id_hint, snippet_type,
                    _target_table_hint or "n/a",
                    _valid_result[1],
                )
                return None
            sql_raw = _valid_result[2] if len(_valid_result) > 2 else sql_raw
            _validation_passed = True
            logger.info(
                "Lever 6 [%s]: snippet validation (kind=%s, target=%s): PASSED",
                _cluster_id_hint, snippet_type,
                _target_table_hint or "n/a",
            )
        else:
            # T2.14: explicit log line for the "no backend" branch so the
            # absence of a PASSED line no longer looks like a silent skip.
            logger.info(
                "Lever 6 [%s]: snippet validation (kind=%s, target=%s): SKIPPED "
                "(no spark/warehouse backend; applier gate will reject unless "
                "validation_passed=True is stamped upstream)",
                _cluster_id_hint, snippet_type,
                _target_table_hint or "n/a",
            )
            # No execution backend: propose without execute-validation.
            # The applier-side gate (Tier 2.8) will reject this unless
            # ``validation_passed=True`` is explicitly set by the caller.
            _validation_passed = False

        from genie_space_optimizer.common.genie_schema import count_sql_snippets, MAX_SQL_SNIPPETS

        current_snippet_count = count_sql_snippets(metadata_snapshot)
        if current_snippet_count >= MAX_SQL_SNIPPETS:
            logger.info(
                "Lever 6: Snippet budget exhausted (%d/%d), skipping",
                current_snippet_count, MAX_SQL_SNIPPETS,
            )
            return None

        existing = metadata_snapshot.get("sql_snippets", {})
        type_key = {"measure": "measures", "filter": "filters", "expression": "expressions"}[snippet_type]
        for existing_item in (existing.get(type_key, []) or []):
            existing_sql = existing_item.get("sql", [])
            existing_sql_str = existing_sql[0] if isinstance(existing_sql, list) and existing_sql else str(existing_sql)
            if _ngram_similarity(sql_raw.lower(), existing_sql_str.lower()) > 0.85:
                logger.info("Lever 6: Duplicate SQL snippet detected, skipping")
                return None

        patch_type_map = {
            "measure": "add_sql_snippet_measure",
            "filter": "add_sql_snippet_filter",
            "expression": "add_sql_snippet_expression",
        }

        static_default = _LEVER_TO_PATCH_TYPE.get((root_cause, 6), "N/A")
        logger.info(
            "Lever 6 LLM chose snippet_type=%s for cluster root_cause=%s "
            "(static default would have been %s)",
            snippet_type, root_cause, static_default,
        )

        span.set_outputs({"snippet_type": snippet_type, "sql": sql_raw[:200]})

        # Run the deterministic naming policy over the LLM result before
        # building the proposal. The qualifier is derived from
        # ``target_table`` (preferred — it is what the LLM was asked to
        # name) with a fallback to parsing the validated SQL. This step
        # is unconditional so prompt drift cannot reintroduce ambiguous
        # ``Month-to-Date Filter``-style names on domain-specific
        # tables.
        _qualified = _qualify_sql_snippet_metadata(
            {
                "snippet_type": snippet_type,
                "sql": sql_raw,
                "display_name": llm_result.get("display_name", ""),
                "instruction": llm_result.get("instruction", ""),
                "target_table": llm_result.get("target_table", ""),
            },
            target_table=str(llm_result.get("target_table", "") or ""),
        )

        proposal = {
            "patch_type": patch_type_map[snippet_type],
            "lever": 6,
            "snippet_type": snippet_type,
            "display_name": _qualified.get("display_name", ""),
            "alias": llm_result.get("alias", ""),
            "sql": sql_raw,
            "synonyms": llm_result.get("synonyms", []),
            "instruction": _qualified.get("instruction", ""),
            "target_table": llm_result.get("target_table", ""),
            "rationale": llm_result.get("rationale", ""),
            "affected_questions": llm_result.get("affected_questions", []),
            "confidence": 0.7,
            # Tier 2.8: validation_passed tracks whether validate_sql_snippet
            # returned a clean EXPLAIN+execute result. The applier refuses
            # to persist add_sql_snippet_* patches without this stamp.
            "questions_fixed": len(cluster.get("question_traces", [])),
            "validation_passed": _validation_passed,
        }

        # Bug #4 firewall — kept in place for forward-compatibility with
        # future patch types routed through this code path, BUT now a
        # no-op for the sql_snippet patch types Lever 6 actually emits.
        # The patch-type dispatch in ``leakage._PATCH_TEXT_FIELDS`` no
        # longer contains ``add_sql_snippet_{measure,filter,expression}``
        # (see scoping docstring there). Rationale:
        #
        #   Lever 6 proposes structural primitives (a measure / filter /
        #   expression), not answer-shaped example_sqls. These are
        #   exec-validated at propose time via ``validate_sql_snippet``
        #   AND go through the post-iteration full-eval arbiter gate
        #   with rollback on regression — a stronger empirical check
        #   than fingerprint-matching against the benchmark corpus.
        #
        # If a future patch type IS added here that persists answer-
        # shaped content (e.g. a hypothetical Lever 6-emitted
        # ``add_example_sql``), it will still be firewalled via its
        # presence in ``_PATCH_TEXT_FIELDS``.
        if benchmarks:
            from genie_space_optimizer.optimization.leakage import (
                BenchmarkCorpus, is_benchmark_leak,
            )
            corpus = BenchmarkCorpus.from_benchmarks(benchmarks)
            is_leak, leak_reason = is_benchmark_leak(
                proposal, proposal["patch_type"], corpus,
            )
            if is_leak:
                _incr_bug4_counter("firewall_rejections")
                logger.info(
                    "Bug #4 firewall: Lever 6 proposal rejected (%s) - %s",
                    proposal["patch_type"], leak_reason,
                )
                return None

        return proposal


# ── Lever 6 / Phase 2: Proactive SQL Expression Mining ─────────────────

_AGG_PATTERN = re.compile(
    r"((?:SUM|COUNT|AVG|MIN|MAX|COUNT\s+DISTINCT)\s*\([^)]+\))",
    re.IGNORECASE,
)

_WHERE_PATTERN = re.compile(
    r"WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+HAVING|\s+LIMIT|\s*$)",
    re.IGNORECASE | re.DOTALL,
)

_DERIVED_PATTERN = re.compile(
    r"(CASE\s+WHEN.+?END|"
    r"(?:MONTH|QUARTER|YEAR|DATE_TRUNC|DATE_FORMAT|CONCAT)\s*\([^)]+\))",
    re.IGNORECASE,
)


# ── Phase 3.R7: alias rewriting for mined SQL expressions ─────────────
#
# Benchmark queries routinely use local aliases:
#   SELECT SUM(F.SALES_AMOUNT_USD) FROM cat.sch.fact_sales F
# When ``_mine_sql_expression_candidates`` extracts ``SUM(F.SALES_AMOUNT_USD)``
# the alias ``F`` is lost, so the stand-alone EXPLAIN fails with
# ``UNRESOLVED_COLUMN: F.SALES_AMOUNT_USD``. These helpers parse the
# source FROM/JOIN clauses, build ``{alias_lower: full_identifier}``,
# and rewrite the extracted expression in place.

_FROM_JOIN_WITH_ALIAS_RE = re.compile(
    # ``FROM``/``JOIN`` + dotted identifier + optional AS / bare alias.
    # Swallows backticks on either side; stops at whitespace or SQL
    # punctuation. Deliberately does not capture subqueries
    # (``FROM (SELECT ...)``) or table-valued expressions (LATERAL,
    # UNNEST, VALUES) — those have no alias to rebind against.
    r"\b(?:FROM|JOIN)\s+"
    r"(?!\(|SELECT\b|LATERAL\b|UNNEST\b|VALUES\b)"
    r"((?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s*\.\s*(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*))*)"
    r"(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*))?",
    re.IGNORECASE,
)

# Reserved words that are not aliases even if they sit in alias position.
_NOT_AN_ALIAS = frozenset({
    "where", "group", "order", "having", "limit", "join", "inner",
    "left", "right", "full", "cross", "outer", "on", "using",
    "natural", "union", "intersect", "except", "qualify", "window",
    "cluster", "distribute", "sort", "lateral", "pivot", "unpivot",
    "with", "as",
})


def _extract_alias_bindings(sql: str) -> dict[str, str]:
    """Parse FROM/JOIN clauses in ``sql`` and return ``{alias_lower:
    full_identifier}``. The full identifier preserves its original case
    (not stripped) so rebinding emits the exact form the schema has on
    disk. When no alias is declared the table name itself is used as
    the key (identity binding), so expressions that qualify by table
    name also validate without rewriting.
    """
    if not sql:
        return {}
    bindings: dict[str, str] = {}
    for m in _FROM_JOIN_WITH_ALIAS_RE.finditer(sql):
        full_ident_raw = m.group(1)
        alias_raw = m.group(2)
        full_ident = ".".join(
            seg.strip().strip("`") for seg in full_ident_raw.split(".")
        )
        if alias_raw and alias_raw.lower() not in _NOT_AN_ALIAS:
            bindings.setdefault(alias_raw.lower(), full_ident)
        # Identity binding: ``FROM cat.sch.t`` → ``t`` also maps to
        # ``cat.sch.t`` so expressions qualifying by bare table name
        # still rebind cleanly.
        short = full_ident.split(".")[-1]
        if short:
            bindings.setdefault(short.lower(), full_ident)
    return bindings


_ALIAS_COL_RE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b"
)


def _rebind_expression_aliases(
    expr: str, alias_map: dict[str, str],
) -> str | None:
    """Rewrite ``alias.col`` references in ``expr`` using ``alias_map``.

    Returns the rewritten expression on success, or ``None`` when any
    referenced alias is missing from the map (the caller drops the
    candidate so EXPLAIN never sees unresolvable references).
    Already-qualified ``cat.sch.t.col`` forms pass through unchanged.
    """
    if not expr:
        return expr
    missing: list[str] = []

    def _replace(m: re.Match[str]) -> str:
        alias = m.group(1)
        col = m.group(2)
        # Already the start of a multi-dotted identifier → skip (the
        # regex is greedy but pattern matching occurs left-to-right; a
        # preceding ``.`` means we're mid-identifier).
        before = m.string[: m.start()]
        if before.endswith("."):
            return m.group(0)
        binding = alias_map.get(alias.lower())
        if binding is None:
            # Uppercase SQL keywords and aggregate function names are
            # not aliases — don't demand them in the map.
            if alias.upper() in {
                "SUM", "COUNT", "AVG", "MIN", "MAX", "CASE",
                "WHEN", "THEN", "ELSE", "END", "AS", "AND", "OR",
                "NOT", "NULL", "TRUE", "FALSE", "DISTINCT", "ALL",
                "BETWEEN", "IN", "IS", "LIKE", "ILIKE",
            }:
                return m.group(0)
            missing.append(alias)
            return m.group(0)
        return f"{binding}.{col}"

    rewritten = _ALIAS_COL_RE.sub(_replace, expr)
    if missing:
        return None
    return rewritten


_MINER_TARGETS: tuple[str, ...] = (
    "sql_snippet", "join_spec", "example_qsql",
    "table_desc", "column_synonym", "keep_in_prose",
)


def _format_existing_join_specs_brief(metadata_snapshot: dict) -> str:
    """Return a terse dedup-hint list of existing join specs for the miner.

    PR 29 — Specs that touch a metric view per ``_asset_semantics`` are
    annotated with ``[METRIC_VIEW: skip]`` so the miner treats them as
    stale rather than valid join hints.
    """
    instr = metadata_snapshot.get("instructions", {})
    specs = instr.get("join_specs", []) if isinstance(instr, dict) else []
    lines: list[str] = []
    for spec in specs or []:
        if not isinstance(spec, dict):
            continue
        left = spec.get("left", {}) if isinstance(spec.get("left"), dict) else {}
        right = spec.get("right", {}) if isinstance(spec.get("right"), dict) else {}
        cond = spec.get("sql", [])
        cond_str = cond[0] if isinstance(cond, list) and cond else str(cond or "")
        left_id = left.get("identifier", "?") if isinstance(left, dict) else "?"
        right_id = right.get("identifier", "?") if isinstance(right, dict) else "?"
        suffix = ""
        if (
            _semantics_is_metric_view_id(metadata_snapshot, left_id)
            or _semantics_is_metric_view_id(metadata_snapshot, right_id)
        ):
            suffix = " [METRIC_VIEW: skip; CTE-first only]"
        lines.append(
            f"- {left_id} ↔ {right_id} :: {cond_str[:120]}{suffix}"
        )
    return "\n".join(lines) if lines else "(none)"


def _format_existing_example_sqls_brief(metadata_snapshot: dict) -> str:
    """Return a terse dedup-hint list of existing example question SQLs."""
    instr = metadata_snapshot.get("instructions", {})
    examples = instr.get("example_question_sqls", []) if isinstance(instr, dict) else []
    lines: list[str] = []
    for ex in examples or []:
        if not isinstance(ex, dict):
            continue
        question = str(ex.get("question", ""))[:120]
        sql = ex.get("sql", "")
        if isinstance(sql, list):
            sql = " ".join(str(s) for s in sql)
        lines.append(f"- Q: {question!r} — SQL: {str(sql)[:120]}")
    return "\n".join(lines) if lines else "(none)"


def _validate_miner_candidate(candidate: Any, instructions_text: str) -> tuple[bool, str]:
    """Structural validation shared by every target.

    Checks the envelope fields (``target``, ``source_span``, ``confidence``,
    ``payload``) — per-target payload validation lives in the dispatcher.
    Returns ``(ok, reason)`` where ``reason`` is a short string used in the
    observability summary on rejection.
    """
    from genie_space_optimizer.common.config import (
        CANONICAL_SECTION_HEADERS, PROMOTE_MIN_CONFIDENCE,
        sql_in_text_findings,
    )

    if not isinstance(candidate, dict):
        return False, "not_a_dict"
    target = str(candidate.get("target", "")).strip()
    if target not in _MINER_TARGETS:
        return False, f"bad_target:{target or 'missing'}"
    span = candidate.get("source_span", "")
    if not isinstance(span, str) or not span.strip():
        return False, "missing_source_span"
    try:
        confidence = float(candidate.get("confidence", 0.0))
    except (TypeError, ValueError):
        return False, "bad_confidence"
    if confidence < PROMOTE_MIN_CONFIDENCE:
        return False, f"low_confidence:{confidence:.2f}"
    payload = candidate.get("payload")
    if not isinstance(payload, dict):
        return False, "missing_payload"

    # ``keep_in_prose`` must specify a canonical section, AND the span
    # must not contain SQL structure (scanner v2 check). ``source_span``
    # can be multi-line (compound bullet + sub-bullets); the single-line
    # ``looks_like_sql_in_prose`` would under-detect, so we use the
    # line-aware ``sql_in_text_findings`` which iterates ``splitlines()``.
    if target == "keep_in_prose":
        section = str(payload.get("section", "")).strip()
        if section not in CANONICAL_SECTION_HEADERS:
            return False, f"keep_in_prose_bad_section:{section[:40]!r}"
        if sql_in_text_findings(span):
            return False, "keep_in_prose_contains_sql"
    return True, "ok"


def _convert_instructions_to_sql_expressions(
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
) -> dict[str, list[dict]]:
    """Multi-target prose rule miner (extension of the legacy SQL-only miner).

    Calls an LLM with :data:`PROSE_RULE_MINING_PROMPT` to classify every
    promotable rule in ``text_instructions`` as one of six targets
    (sql_snippet, join_spec, example_qsql, table_desc, column_synonym,
    keep_in_prose). Each candidate carries a ``source_span`` — an exact
    substring of the input prose that the rewrite step later removes.

    This keeps the legacy function name so existing callers compile, but
    the shape of the return value is now a dict keyed by target rather
    than a flat ``list[dict]`` of SQL snippets. Callers that only care
    about SQL snippets should read ``result["sql_snippet"]``.

    Pipeline per call:

    1. Build context (schema digest + existing structured config for
       dedup hints).
    2. LLM call with structured-JSON retry (B.2): on parse failure,
       re-invoke with a repair prompt asking for a plain JSON array.
    3. Structural validation (envelope + confidence gate).
    4. Per-target payload validation (including EXPLAIN for sql_snippet
       / join_spec / example_qsql, dedup against existing config).
    5. Observability summary: single INFO line with counts by target
       and rejection reasons.

    Returns a dict with exactly these keys (never missing, may be empty):

    - ``sql_snippet`` : ``list[dict]`` — each with the legacy shape so
      :func:`_apply_instruction_sql_expressions` still consumes it.
    - ``join_spec``    : ``list[dict]`` — shape expected by the join
      spec applier (Task C.3).
    - ``example_qsql`` : ``list[dict]`` — question / SQL / usage.
    - ``table_desc``   : ``list[dict]`` — identifier + description_append.
    - ``column_synonym``: ``list[dict]`` — identifier + column + synonyms.
    - ``keep_in_prose``: ``list[dict]`` — section + source_span (used by
      the rewrite step to regroup content under canonical headers).
    - ``stats``        : ``dict`` — observability (candidates_total,
      promoted_by_target, rejected_by_reason, retries).
    """
    from genie_space_optimizer.common.config import (
        PROSE_RULE_MINING_PROMPT, format_mlflow_template,
    )
    from genie_space_optimizer.optimization.applier import _get_general_instructions
    from genie_space_optimizer.optimization.benchmarks import (
        validate_ground_truth_sql, validate_sql_snippet,
    )
    from genie_space_optimizer.optimization.evaluation import _extract_json

    empty: dict[str, list[dict]] = {t: [] for t in _MINER_TARGETS}
    empty["stats"] = {
        "candidates_total": 0, "promoted_by_target": {},
        "rejected_by_reason": {}, "retries": 0,
    }

    instructions_text = _get_general_instructions(metadata_snapshot)
    if not instructions_text or len(instructions_text.strip()) < 20:
        logger.info("miner: no substantial instructions to mine")
        return empty

    # ── Build prompt context ────────────────────────────────────────
    ds = metadata_snapshot.get("data_sources", {})
    all_sources: list = []
    if isinstance(ds, dict):
        all_sources.extend(ds.get("tables", []) or [])
        all_sources.extend(ds.get("metric_views", []) or [])
    from genie_space_optimizer.optimization.archetypes import _col_type
    schema_lines: list[str] = []
    for t in all_sources:
        if not isinstance(t, dict):
            continue
        tname = t.get("identifier", t.get("name", "")).split(".")[-1]
        for col in (t.get("columns", []) or []):
            cname = col.get("name", "")
            ctype = _col_type(col)
            desc = col.get("description", "")[:80]
            if cname:
                schema_lines.append(f"{tname}.{cname} ({ctype}): {desc}")
        for cc in (t.get("column_configs", []) or []):
            cname = cc.get("column_name", "")
            if cname and not any(cname in ln for ln in schema_lines):
                schema_lines.append(f"{tname}.{cname}: (from column_configs)")
    schema_context = "\n".join(schema_lines) if schema_lines else "(no schema available)"

    instr = metadata_snapshot.get("instructions", {})
    existing_snippets = instr.get("sql_snippets", {}) if isinstance(instr, dict) else {}
    existing_sql_strs: list[str] = []
    for category in ("measures", "filters", "expressions"):
        for item in existing_snippets.get(category, []):
            sql_raw = item.get("sql", "")
            sql_str = (
                "".join(str(s) for s in sql_raw).strip()
                if isinstance(sql_raw, list) else str(sql_raw).strip()
            )
            if sql_str:
                existing_sql_strs.append(f"{category}: {sql_str}")
    existing_expressions = "\n".join(existing_sql_strs) or "(none)"

    prompt = format_mlflow_template(
        PROSE_RULE_MINING_PROMPT,
        instructions_text=instructions_text,
        schema_context=schema_context,
        existing_expressions=existing_expressions,
        existing_join_specs=_format_existing_join_specs_brief(metadata_snapshot),
        existing_example_sqls=_format_existing_example_sqls_brief(metadata_snapshot),
    )

    # ── LLM call with JSON-repair retry ─────────────────────────────
    system_msg = (
        "You are a Databricks Genie Space configuration expert. "
        "Respond with a single JSON array and nothing else."
    )
    raw_text = ""
    candidates_raw: list[Any] | None = None
    retries = 0
    for attempt in range(2):
        try:
            text, _response = _traced_llm_call(
                w, system_msg, prompt,
                span_name="prose_rule_mining" if attempt == 0 else "prose_rule_mining_retry",
            )
        except Exception:
            logger.warning(
                "miner: LLM call failed (attempt=%d)", attempt + 1, exc_info=True,
            )
            continue
        raw_text = text or ""
        try:
            parsed = _extract_json(raw_text)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "miner: JSON parse failed (attempt=%d): %s. raw=%r",
                attempt + 1, exc, raw_text[:200],
            )
            if attempt == 0:
                # Build a one-shot repair prompt that asks the LLM to
                # emit JUST the JSON — no prose, no fences.
                prompt = (
                    "Your previous reply could not be parsed as JSON. "
                    "Return ONLY the JSON array — no prose, no markdown "
                    "code fences, no preamble.\n\n"
                    "Original task:\n" + prompt
                )
                retries += 1
                continue
            return empty
        if isinstance(parsed, list):
            candidates_raw = parsed
        elif isinstance(parsed, dict):
            # Tolerate wrapped shape: {"candidates": [...]} or similar.
            for key in ("candidates", "rules", "expressions"):
                val = parsed.get(key)
                if isinstance(val, list):
                    candidates_raw = val
                    break
        if candidates_raw is None:
            logger.warning(
                "miner: JSON did not contain an array (attempt=%d)",
                attempt + 1,
            )
            if attempt == 0:
                retries += 1
                continue
            return empty
        break

    if candidates_raw is None:
        return empty

    # ── Per-target dispatch and validation ──────────────────────────
    # Observability contract (see Task C.2 in the plan):
    # - ``by_target`` counts raw LLM output distribution — one bump per
    #   candidate BEFORE any validation (envelope + per-target).
    # - ``promoted_by_target`` counts only candidates that survive ALL
    #   checks and landed in a bucket.
    # - ``rejected_by_reason`` buckets rejections for a grep-friendly
    #   diagnostic; the first rejection per target is logged at INFO.
    buckets: dict[str, list[dict]] = {t: [] for t in _MINER_TARGETS}
    by_target: dict[str, int] = {}
    promoted_by_target: dict[str, int] = {}
    rejected_by_reason: dict[str, int] = {}
    # Keyed by the candidate's raw target so we log one sample per target,
    # per the plan ("First rejection reason per target at INFO").
    first_rejection_by_target: set[str] = set()

    existing_sql_lower = {s.lower().strip() for s in existing_sql_strs}

    for c in candidates_raw:
        # Raw-target bump BEFORE validation — safe because the envelope
        # validator guarantees ``target`` is one of the six valid slugs
        # for every candidate that reaches promotion. For rejects whose
        # target is missing / invalid, we attribute to ``"_unknown"``.
        _raw_target = (
            c.get("target") if isinstance(c, dict) and isinstance(c.get("target"), str)
            else "_unknown"
        )
        by_target[_raw_target] = by_target.get(_raw_target, 0) + 1

        ok, reason = _validate_miner_candidate(c, instructions_text)
        if not ok:
            rejected_by_reason[reason] = rejected_by_reason.get(reason, 0) + 1
            if _raw_target not in first_rejection_by_target:
                logger.info(
                    "miner.reject target=%s reason=%s sample=%r",
                    _raw_target, reason, str(c)[:200],
                )
                first_rejection_by_target.add(_raw_target)
            continue

        target = c["target"]
        payload = c["payload"]
        span = c["source_span"]
        confidence = float(c["confidence"])
        # Per-candidate DEBUG line per plan — useful for local diagnosis
        # without flooding INFO in steady state.
        logger.debug(
            "miner.candidate target=%s confidence=%.2f span=%r",
            target, confidence, (span or "")[:80],
        )

        if target == "sql_snippet":
            sql = str(payload.get("sql", "")).strip()
            snippet_type = str(payload.get("snippet_type", "")).strip().lower()
            if not sql or snippet_type not in ("measure", "filter", "expression"):
                rejected_by_reason["sql_bad_shape"] = rejected_by_reason.get("sql_bad_shape", 0) + 1
                continue
            if sql.lower().strip() in existing_sql_lower:
                rejected_by_reason["sql_duplicate"] = rejected_by_reason.get("sql_duplicate", 0) + 1
                continue
            valid = validate_sql_snippet(
                sql, snippet_type, metadata_snapshot,
                spark=spark, catalog=catalog, gold_schema=gold_schema,
                w=w, warehouse_id=warehouse_id,
            )
            if not valid[0]:
                rejected_by_reason["sql_invalid"] = rejected_by_reason.get("sql_invalid", 0) + 1
                # T2.14: surface miner-path validation failures at INFO as
                # well, so the loop log isn't silent about dropped
                # snippets. The debug line is retained for full detail.
                logger.info(
                    "Lever 6 [miner]: snippet validation (kind=%s): FAILED — %s",
                    snippet_type, valid[1],
                )
                logger.debug("miner: sql_snippet rejected: %s — %s", sql[:80], valid[1])
                continue
            logger.info(
                "Lever 6 [miner]: snippet validation (kind=%s): PASSED",
                snippet_type,
            )
            prefixed_sql = valid[2] if len(valid) > 2 else sql
            # Apply the deterministic naming policy. The prose miner
            # asks the LLM for ``display_name`` + ``description``; the
            # qualifier prefixes domain-specific tables and backfills
            # an instruction hint when neither was supplied.
            _candidate = _qualify_sql_snippet_metadata({
                "snippet_type": snippet_type,
                "sql": prefixed_sql,
                "display_name": payload.get("display_name", ""),
                "description": payload.get("description", ""),
                "synonyms": payload.get("synonyms", []) or [],
                "alias": payload.get("alias", ""),
                "is_default": bool(payload.get("is_default", False)),
                "omit_when": payload.get("omit_when"),
                "source": "instruction_derived",
                "source_span": span,
                "confidence": confidence,
            })
            buckets["sql_snippet"].append(_candidate)

        elif target == "join_spec":
            left = payload.get("left", {}) if isinstance(payload.get("left"), dict) else {}
            right = payload.get("right", {}) if isinstance(payload.get("right"), dict) else {}
            sql_field = payload.get("sql", [])
            if not isinstance(sql_field, list) or len(sql_field) < 1:
                rejected_by_reason["join_bad_shape"] = rejected_by_reason.get("join_bad_shape", 0) + 1
                continue
            if not (left.get("identifier") and right.get("identifier")):
                rejected_by_reason["join_missing_identifier"] = rejected_by_reason.get("join_missing_identifier", 0) + 1
                continue
            buckets["join_spec"].append({
                "left": {"identifier": left.get("identifier"), "alias": left.get("alias") or left.get("identifier", "").split(".")[-1]},
                "right": {"identifier": right.get("identifier"), "alias": right.get("alias") or right.get("identifier", "").split(".")[-1]},
                "sql": sql_field,
                "instruction": payload.get("instruction", ""),
                "source": "instruction_derived",
                "source_span": span,
                "confidence": confidence,
            })

        elif target == "example_qsql":
            question = str(payload.get("question", "")).strip()
            sql = str(payload.get("sql", "")).strip()
            if not question or not sql:
                rejected_by_reason["example_bad_shape"] = rejected_by_reason.get("example_bad_shape", 0) + 1
                continue
            # EXPLAIN via the ground-truth SQL validator (see Task C.2
            # in the plan). We run without ``execute=True`` so this is
            # EXPLAIN-only — fast, deterministic, no data dependency.
            # When no execution backend is available (unit test, offline
            # run), validation short-circuits to structural checks.
            try:
                is_valid, err = validate_ground_truth_sql(
                    sql, spark=spark, catalog=catalog, gold_schema=gold_schema,
                    execute=False, w=w, warehouse_id=warehouse_id,
                )
            except Exception as exc:
                # Validator threw — treat as shape error so we don't
                # promote an unchecked SQL into the space.
                logger.debug("miner: example_qsql validator raised: %s", exc)
                is_valid, err = False, f"validator_error: {exc}"
            if not is_valid and (spark is not None or warehouse_id):
                rejected_by_reason["example_invalid"] = rejected_by_reason.get("example_invalid", 0) + 1
                logger.debug(
                    "miner: example_qsql rejected: %s — %s", sql[:80], err,
                )
                continue
            # Dedup against existing example_question_sqls (normalised by
            # lower-casing the SQL body).
            _instr = metadata_snapshot.get("instructions", {}) or {}
            _existing_examples = _instr.get("example_question_sqls", []) or []
            _existing_sqls_lower: set[str] = set()
            for ex in _existing_examples:
                if not isinstance(ex, dict):
                    continue
                s = ex.get("sql", "")
                if isinstance(s, list):
                    s = " ".join(str(x) for x in s)
                s = str(s).strip().lower()
                if s:
                    _existing_sqls_lower.add(s)
            if sql.lower().strip() in _existing_sqls_lower:
                rejected_by_reason["example_duplicate"] = rejected_by_reason.get("example_duplicate", 0) + 1
                continue
            buckets["example_qsql"].append({
                "question": question,
                "sql": sql,
                "usage_guidance": payload.get("usage_guidance", ""),
                "source": "instruction_derived",
                "source_span": span,
                "confidence": confidence,
            })

        elif target == "table_desc":
            tid = str(payload.get("table_identifier", "")).strip()
            desc_append = str(payload.get("description_append", "")).strip()
            if not tid or not desc_append:
                rejected_by_reason["table_desc_bad_shape"] = rejected_by_reason.get("table_desc_bad_shape", 0) + 1
                continue
            buckets["table_desc"].append({
                "table_identifier": tid,
                "description_append": desc_append,
                "source": "instruction_derived",
                "source_span": span,
                "confidence": confidence,
            })

        elif target == "column_synonym":
            tid = str(payload.get("table_identifier", "")).strip()
            col = str(payload.get("column_name", "")).strip()
            syns = payload.get("synonyms", []) or []
            if not tid or not col or not isinstance(syns, list) or not syns:
                rejected_by_reason["synonym_bad_shape"] = rejected_by_reason.get("synonym_bad_shape", 0) + 1
                continue
            buckets["column_synonym"].append({
                "table_identifier": tid,
                "column_name": col,
                "synonyms": [str(s) for s in syns if str(s).strip()],
                "source": "instruction_derived",
                "source_span": span,
                "confidence": confidence,
            })

        else:  # keep_in_prose
            buckets["keep_in_prose"].append({
                "section": payload["section"],
                "source_span": span,
                "confidence": confidence,
            })

        promoted_by_target[target] = promoted_by_target.get(target, 0) + 1

    stats = {
        "candidates_total": len(candidates_raw),
        "by_target": by_target,  # raw LLM output distribution
        "promoted_by_target": promoted_by_target,  # after all validators
        "rejected_by_reason": rejected_by_reason,
        "retries": retries,
    }
    # Single structured summary line for the run log. Keep it parseable
    # by grep: the k=v format survives log aggregators better than JSON.
    # Matches the format in Task C.2 ("Observability") of the plan.
    logger.info(
        "miner.summary candidates_total=%d by_target=%s promoted=%s "
        "rejected=%s retries=%d",
        stats["candidates_total"],
        by_target,
        promoted_by_target,
        rejected_by_reason,
        retries,
    )
    return {**buckets, "stats": stats}


def _mine_sql_expression_candidates(
    benchmarks: list[dict],
    metadata_snapshot: dict,
) -> list[dict]:
    """Extract SQL Expression candidates from benchmark ground-truth SQL.

    Scans all benchmark expected_sql for:
      - Recurring aggregation patterns (2+ occurrences) -> measures
      - Recurring WHERE patterns (2+ occurrences) -> filters
      - Recurring derived columns (CASE, date functions) -> expressions

    Returns list of candidate dicts with:
      {snippet_type, sql, display_name, alias, source_count}
    """
    from collections import Counter

    from genie_space_optimizer.common.config import SQL_EXPRESSION_MIN_FREQUENCY

    agg_counter: Counter[str] = Counter()
    where_counter: Counter[str] = Counter()
    derived_counter: Counter[str] = Counter()

    # Phase 3.R7: drops counter lives on the function-local attribute so
    # the caller can surface it in the SQL expression seeding summary
    # without changing the return contract. Reset per call.
    rebind_dropped = 0
    rebind_dropped_examples: list[str] = []

    def _rebind_or_none(
        expr: str, alias_map: dict[str, str], upper: bool,
    ) -> str | None:
        rewritten = _rebind_expression_aliases(expr, alias_map)
        if rewritten is None:
            return None
        normalized = " ".join(rewritten.split())
        return normalized.upper() if upper else normalized

    for b in benchmarks:
        sql = b.get("expected_sql", "") or ""
        if not sql.strip():
            continue
        alias_map = _extract_alias_bindings(sql)

        for m in _AGG_PATTERN.findall(sql):
            rebound = _rebind_or_none(m, alias_map, upper=True)
            if rebound is None:
                rebind_dropped += 1
                if len(rebind_dropped_examples) < 3:
                    rebind_dropped_examples.append(m[:80])
                continue
            agg_counter[rebound] += 1

        where_match = _WHERE_PATTERN.search(sql)
        if where_match:
            clause = where_match.group(1).strip()
            for condition in re.split(r"\s+AND\s+", clause, flags=re.IGNORECASE):
                condition = condition.strip()
                if condition and len(condition) < 200:
                    rebound = _rebind_or_none(condition, alias_map, upper=False)
                    if rebound is None:
                        rebind_dropped += 1
                        if len(rebind_dropped_examples) < 3:
                            rebind_dropped_examples.append(condition[:80])
                        continue
                    where_counter[rebound] += 1

        for m in _DERIVED_PATTERN.findall(sql):
            rebound = _rebind_or_none(m, alias_map, upper=False)
            if rebound is None:
                rebind_dropped += 1
                if len(rebind_dropped_examples) < 3:
                    rebind_dropped_examples.append(m[:80])
                continue
            derived_counter[rebound] += 1

    # Stash on function attributes so the harness caller can surface the
    # counts in the seeding summary block without changing the return
    # shape (the callers that don't read these get zeros by default).
    _mine_sql_expression_candidates.last_rebind_dropped = rebind_dropped
    _mine_sql_expression_candidates.last_rebind_dropped_examples = (
        rebind_dropped_examples
    )

    candidates: list[dict] = []

    for sql_expr, count in agg_counter.most_common():
        if count < SQL_EXPRESSION_MIN_FREQUENCY:
            break
        alias = re.sub(r"[^a-z0-9]+", "_", sql_expr.lower()).strip("_")[:50]
        candidates.append({
            "snippet_type": "measure",
            "sql": sql_expr,
            "display_name": _auto_display_name(sql_expr, "measure"),
            "alias": alias,
            "source_count": count,
        })

    for sql_expr, count in where_counter.most_common():
        if count < SQL_EXPRESSION_MIN_FREQUENCY:
            break
        candidates.append({
            "snippet_type": "filter",
            "sql": sql_expr,
            "display_name": _auto_display_name(sql_expr, "filter"),
            "alias": "",
            "source_count": count,
        })

    for sql_expr, count in derived_counter.most_common():
        if count < SQL_EXPRESSION_MIN_FREQUENCY:
            break
        alias = re.sub(r"[^a-z0-9]+", "_", sql_expr.lower()).strip("_")[:50]
        candidates.append({
            "snippet_type": "expression",
            "sql": sql_expr,
            "display_name": _auto_display_name(sql_expr, "expression"),
            "alias": alias,
            "source_count": count,
        })

    seen: set[str] = set()
    deduped: list[dict] = []
    for c in candidates:
        key = c["sql"].lower()
        if any(_ngram_similarity(key, s) > 0.85 for s in seen):
            continue
        seen.add(key)
        deduped.append(c)

    # Apply the deterministic naming policy. Benchmark candidates have
    # no explicit ``target_table`` (the miner extracts patterns rather
    # than scanning schema), so the helper falls back to parsing the
    # first FQ identifier out of ``sql``.
    return [_qualify_sql_snippet_metadata(c) for c in deduped]


def _auto_display_name(sql: str, snippet_type: str) -> str:
    """Generate a human-readable display name from a SQL expression."""
    sql_clean = sql.strip()

    if snippet_type == "measure":
        match = re.match(r"(SUM|COUNT|AVG|MIN|MAX)\s*\((.+)\)", sql_clean, re.IGNORECASE)
        if match:
            func, col = match.group(1).upper(), match.group(2).strip()
            col_name = col.split(".")[-1].replace("_", " ").title()
            prefix = {"SUM": "Total", "COUNT": "Count of", "AVG": "Average",
                       "MIN": "Minimum", "MAX": "Maximum"}.get(func, func)
            return f"{prefix} {col_name}"
        return f"Measure: {sql_clean[:40]}"

    if snippet_type == "filter":
        return f"Filter: {sql_clean[:50]}"

    if snippet_type == "expression":
        match = re.match(r"(MONTH|QUARTER|YEAR|DATE_TRUNC)\s*\((.+)\)", sql_clean, re.IGNORECASE)
        if match:
            func, col = match.group(1).title(), match.group(2).strip()
            col_name = col.split(".")[-1].replace("_", " ").title()
            return f"{col_name} {func}"
        return f"Expression: {sql_clean[:40]}"

    return sql_clean[:50]


# ── SQL Expression naming disambiguation policy ────────────────────────
#
# Three SQL Expression population paths (proactive seeding, reactive
# Lever 6 proposals, and prose mining) all produce ``display_name`` /
# ``instruction`` metadata. Without a deterministic post-processing
# step, names like ``Month-to-Date Filter`` end up applied to multiple
# domain-specific tables in the same Genie Space (e.g. ``mv_<domain_a>_*``
# vs ``mv_<domain_b>_*`` fact tables), which makes Genie's snippet
# selection ambiguous.
#
# The helpers below are the enforcement layer. Prompts can drift, but
# this code runs unconditionally after enrichment. Pattern matching is
# delegated to :mod:`genie_space_optimizer.common.naming` so the
# leaf-prefix vocabulary stays in one place.

from genie_space_optimizer.common.naming import (  # noqa: E402 — sibling helper
    DEFAULT_DOMAIN_PREFIX_RE as _MV_DOMAIN_PREFIX_RE,  # backwards-compat alias
    domain_qualifier_from_identifier as _domain_qualifier_from_identifier_impl,
    schema_qualifier_from_identifier,
)

# Regex that finds three-or-more-part fully-qualified identifiers (e.g.
# ``catalog.schema.table``). Anchored on word boundaries so it doesn't
# also match plain column references that happen to share a word
# segment with a table name.
_FQ_IDENTIFIER_RE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){2,})\b"
)


def _extract_primary_table_identifier(sql: str) -> str:
    """Return the first three-part identifier referenced by ``sql``.

    The benchmark miner does not attach explicit ``target_table``
    metadata to its candidates, so the qualifier helper falls back to
    parsing the SQL text itself. We pick the first occurrence to keep
    the rule predictable; ties are exceptionally rare for a single
    SQL Expression because they typically reference one fact table.
    """
    if not sql:
        return ""
    match = _FQ_IDENTIFIER_RE.search(sql)
    if not match:
        return ""
    full = match.group(1)
    # ``catalog.schema.table.column`` → drop the trailing column part
    # so callers see only the table identifier.
    parts = full.split(".")
    if len(parts) >= 4:
        return ".".join(parts[:3])
    return full


def _domain_qualifier_from_identifier(
    identifier: str,
    *,
    distinct_schemas: int = 0,
) -> str:
    """Return the compact source qualifier for ``identifier``.

    Resolves in two passes:

    1. Leaf-prefix match against the broadened
       ``mv|vw|f|d|stg|...|view_<domain>_*`` vocabulary in
       :mod:`common.naming` (plus any user-supplied patterns from
       ``GSO_DOMAIN_TABLE_PATTERNS``).
    2. Schema fallback when no leaf prefix matches AND the space has
       multiple distinct schemas (``distinct_schemas >= 2``). The
       fallback uses the schema portion of the dotted identifier so
       e.g. ``cat.orders.fact_lines`` becomes ``ORDERS``.

    Generic table names with no recognized prefix in a single-schema
    space receive no qualifier so we don't add noisy artificial
    prefixes like ``T `` to a ``Total Revenue`` measure on a plain
    ``cat.sch.t`` table.
    """
    qualifier = _domain_qualifier_from_identifier_impl(identifier)
    if qualifier:
        return qualifier
    return schema_qualifier_from_identifier(
        identifier, distinct_schemas=distinct_schemas
    )


def _qualify_sql_snippet_metadata(
    candidate: dict,
    *,
    target_table: str = "",
) -> dict:
    """Return a copy of ``candidate`` with qualified naming metadata.

    Applies the SQL Expression naming disambiguation policy:

    - Adds the compact domain qualifier (e.g. ``ORDERS`` extracted
      from ``mv_orders_fact_lines``) to ``display_name`` when the
      candidate references a domain-specific table and the name does
      not already start with the qualifier.
    - Backfills an empty / blank ``instruction`` so Genie picks up the
      "when to use this" hint with source-aware text.
    - Returns a new dict so callers can safely substitute the
      qualified copy without mutating shared candidate state.

    The helper is path-agnostic: it works for benchmark-mined,
    schema-discovered, LLM-enriched, prose-mined, and Lever 6
    proposals. The only inputs it needs are ``sql``, ``display_name``,
    optionally ``target_table`` (or one passed explicitly), and
    ``instruction`` (read from either ``instruction`` or
    ``description`` so the prose mining payload shape is supported).
    """
    if not isinstance(candidate, dict):
        return candidate

    out = dict(candidate)

    sql_field = out.get("sql", "")
    if isinstance(sql_field, list):
        sql_text = sql_field[0] if sql_field else ""
    else:
        sql_text = str(sql_field or "")

    table = (
        target_table
        or str(out.get("target_table", "") or "")
        or _extract_primary_table_identifier(sql_text)
    )
    qualifier = _domain_qualifier_from_identifier(table)

    if qualifier:
        display_name = str(out.get("display_name", "") or "").strip()
        if display_name and not display_name.upper().startswith(
            qualifier.upper() + " "
        ):
            out["display_name"] = f"{qualifier} {display_name}"
        elif not display_name:
            out["display_name"] = qualifier

    instruction_value = out.get("instruction", out.get("description", ""))
    instruction_text = ""
    if isinstance(instruction_value, list):
        instruction_text = " ".join(
            str(item).strip() for item in instruction_value if item
        ).strip()
    else:
        instruction_text = str(instruction_value or "").strip()

    if not instruction_text and (qualifier or table):
        # Backfill so Genie has a "when to use this" hint that mentions
        # the source domain. Keep it short and source-aware — this is
        # only a fallback when neither the LLM nor the prose miner
        # produced explicit instruction text.
        snippet_type = str(out.get("snippet_type", "")).strip().lower()
        kind = {
            "measure": "measure",
            "filter": "filter",
            "expression": "expression",
        }.get(snippet_type, "SQL expression")
        if qualifier:
            out["instruction"] = (
                f"Use this {kind} when answering questions about "
                f"{qualifier} ({table})."
            )
        else:
            out["instruction"] = (
                f"Use this {kind} when answering questions about {table}."
            )

    return out


def _discover_schema_sql_expressions(
    metadata_snapshot: dict,
) -> list[dict]:
    """Discover SQL Expression candidates from schema patterns.

    Scans column names, types, and descriptions for strong signals:
      - Numeric columns named like revenue/cost/amount -> SUM measures
      - Date/timestamp columns -> MONTH/QUARTER expressions

    Reads columns from ``column_configs`` (the serialized_space field the
    Genie API populates) with a legacy fallback to ``columns``. Also
    scans ``data_sources.metric_views`` so dimensional MVs with date or
    numeric columns contribute candidates too. Historically this
    function only read ``columns`` (never populated in production), so
    the entire schema-discovery source quietly produced zero candidates
    and the seeding pool collapsed to benchmark mining.

    Returns conservative candidates that still need execution validation.
    """
    _MEASURE_PATTERNS = re.compile(
        r"(?:revenue|sales|amount|total|cost|expense|price|profit|margin|"
        r"count|qty|quantity|fee|charge|discount|balance)",
        re.IGNORECASE,
    )
    _DATE_PATTERNS = re.compile(
        r"(?:date|_at$|_on$|timestamp|datetime|created|updated|modified)",
        re.IGNORECASE,
    )
    _NUMERIC_TYPES = {"int", "integer", "bigint", "float", "double", "decimal", "numeric", "long", "short"}

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        return []

    candidates: list[dict] = []
    seen_sqls: set[str] = set()

    # Visit tables AND metric_views — both can contribute mining-worthy
    # numeric/date columns on real spaces (e.g. ``mv_<domain>_fact_<entity>``
    # plus ``mv_<domain>_dim_date``). Use the shared semantics split so
    # table-shelf metric views are reclassified before SQL seeding.
    try:
        from genie_space_optimizer.common.asset_semantics import (
            asset_semantics_entry,
            effective_data_source_split,
        )
        split = effective_data_source_split(metadata_snapshot)
        sources_with_kind = (
            [(src, "table") for src in split.tables]
            + [(src, "metric_view") for src in split.metric_views]
        )
    except Exception:
        logger.debug(
            "Schema SQL expression discovery falling back to raw shelves",
            exc_info=True,
        )
        sources_with_kind = (
            [(src, "table") for src in list(ds.get("tables", []) or [])]
            + [(src, "metric_view") for src in list(ds.get("metric_views", []) or [])]
        )

        def asset_semantics_entry(*_args, **_kwargs):  # type: ignore[no-redef]
            return None

    def _semantic_measure_names(identifier: str) -> set[str]:
        try:
            entry = asset_semantics_entry(metadata_snapshot, identifier)
        except Exception:
            entry = None
        if not isinstance(entry, dict):
            return set()
        return {
            str(m).lower()
            for m in (entry.get("measures") or [])
            if isinstance(m, str) and m.strip()
        }

    for source, source_kind in sources_with_kind:
        if not isinstance(source, dict):
            continue
        table_id = source.get("identifier", "") or source.get("name", "")
        if not table_id:
            continue

        semantic_measures = _semantic_measure_names(table_id)

        # Prefer ``column_configs`` (production shape) and fall back to
        # ``columns`` so legacy fixtures and internal-normalized snapshots
        # continue to work.
        columns = (
            source.get("column_configs")
            or source.get("columns")
            or []
        )
        for col in columns:
            if not isinstance(col, dict):
                continue
            col_name = col.get("column_name", "") or col.get("name", "")
            col_type = (col.get("data_type", "") or col.get("type_text", "") or "").lower()
            if not col_name:
                continue

            is_hidden = col.get("is_hidden", False) or col.get("hidden", False)
            if is_hidden:
                continue

            base_type = col_type.split("(")[0].strip()
            fq_col = f"{table_id}.{col_name}"

            is_semantic_mv_measure = (
                source_kind == "metric_view"
                and col_name.lower() in semantic_measures
            )
            if (
                base_type in _NUMERIC_TYPES
                and _MEASURE_PATTERNS.search(col_name)
                and not is_semantic_mv_measure
            ):
                sql_expr = f"SUM({fq_col})"
                if sql_expr.lower() not in seen_sqls:
                    seen_sqls.add(sql_expr.lower())
                    alias = re.sub(r"[^a-z0-9]+", "_", f"total_{col_name}".lower()).strip("_")[:50]
                    candidates.append({
                        "snippet_type": "measure",
                        "sql": sql_expr,
                        "display_name": f"Total {col_name.replace('_', ' ').title()}",
                        "alias": alias,
                        "source_count": 0,
                        "target_table": table_id,
                    })

            if _DATE_PATTERNS.search(col_name) and ("date" in base_type or "timestamp" in base_type):
                for func, label in [("MONTH", "Month"), ("QUARTER", "Quarter")]:
                    sql_expr = f"{func}({fq_col})"
                    if sql_expr.lower() not in seen_sqls:
                        seen_sqls.add(sql_expr.lower())
                        alias = re.sub(
                            r"[^a-z0-9]+", "_",
                            f"{col_name}_{func}".lower(),
                        ).strip("_")[:50]
                        candidates.append({
                            "snippet_type": "expression",
                            "sql": sql_expr,
                            "display_name": f"{col_name.replace('_', ' ').title()} {label}",
                            "alias": alias,
                            "source_count": 0,
                            "target_table": table_id,
                        })

    # Apply the deterministic naming policy now that ``target_table``
    # is attached. For domain-specific tables (``mv_<domain>_*``,
    # ``vw_<domain>_*``, ``f_<domain>_*``, …) this prefixes the
    # display name with the qualifier; generic tables are left
    # untouched.
    return [_qualify_sql_snippet_metadata(c) for c in candidates]


def _enrich_candidates_with_llm(
    candidates: list[dict],
    metadata_snapshot: dict,
    *,
    w: WorkspaceClient | None = None,
) -> list[dict]:
    """Use LLM to generate display_name, synonyms, and instruction for candidates.

    Takes raw (sql, snippet_type) candidates and enriches them with
    human-friendly metadata.  If the LLM call fails, candidates are returned
    with auto-generated display names.
    """
    if not candidates or w is None:
        return candidates

    import json as _json

    from genie_space_optimizer.common.config import (
        SQL_EXPRESSION_SEEDING_PROMPT,
        format_mlflow_template,
    )

    candidates_json = _json.dumps(
        [{"snippet_type": c["snippet_type"], "sql": c["sql"]} for c in candidates],
        indent=2,
    )
    schema_context = _format_schema_index(metadata_snapshot)

    prompt = format_mlflow_template(
        SQL_EXPRESSION_SEEDING_PROMPT,
        candidates=candidates_json,
        schema=schema_context[:4000],
    )

    try:
        raw_text, _ = _traced_llm_call(
            w, "You are a SQL metadata expert.", prompt,
            span_name="sql_expression_seeding_llm",
        )
        from genie_space_optimizer.optimization.evaluation import _extract_json
        enrichments = _extract_json(raw_text)
        if isinstance(enrichments, list) and len(enrichments) == len(candidates):
            for c, e in zip(candidates, enrichments):
                if isinstance(e, dict):
                    if e.get("display_name"):
                        c["display_name"] = e["display_name"]
                    if e.get("synonyms"):
                        c["synonyms"] = e["synonyms"]
                    if e.get("instruction"):
                        c["instruction"] = e["instruction"]
                    if e.get("alias") and c["snippet_type"] != "filter":
                        c["alias"] = e["alias"]
    except Exception:
        logger.warning("LLM enrichment for SQL expression candidates failed; using auto-generated names", exc_info=True)

    for c in candidates:
        c.setdefault("synonyms", [])
        c.setdefault("instruction", "")

    # Run the deterministic qualifier as a post-processing pass, so a
    # generic LLM-supplied ``display_name`` (e.g. ``Month-to-Date
    # Filter``) for a domain-specific SQL still ends up qualified.
    # This is the safety net for prompt drift / failures and is cheap
    # to run.
    return [_qualify_sql_snippet_metadata(c) for c in candidates]


def _filter_no_op_proposals(proposals: list[dict], metadata_snapshot: dict) -> list[dict]:
    """Remove proposals that don't meaningfully change the current metadata.

    For ``update_column_description`` / ``update_description``: drops the
    proposal if the proposed value is essentially identical to the existing
    column description (after normalizing whitespace).
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    existing_descs: dict[tuple[str, str], str] = {}
    for tbl in tables:
        tbl_name = (tbl.get("name") or tbl.get("identifier") or "").lower()
        short_name = tbl_name.rsplit(".", 1)[-1] if "." in tbl_name else tbl_name
        for col in tbl.get("columns", tbl.get("column_configs", [])):
            col_name = (col.get("name") or col.get("column_name") or "").lower()
            raw_desc = col.get("description") or col.get("comment") or ""
            if isinstance(raw_desc, list):
                raw_desc = " ".join(str(x) for x in raw_desc)
            desc = re.sub(r"\s+", " ", str(raw_desc).strip().lower())
            if desc:
                existing_descs[(tbl_name, col_name)] = desc
                existing_descs[(short_name, col_name)] = desc

    existing_eqs_raw = _get_existing_example_sqls(metadata_snapshot)
    existing_eq_questions: set[str] = set()
    for e in existing_eqs_raw:
        if isinstance(e, dict):
            q = e.get("question", "")
            if isinstance(q, list):
                q = " ".join(q)
            q = q.lower().strip()
            if q:
                existing_eq_questions.add(q)

    kept: list[dict] = []
    dropped = 0
    for p in proposals:
        ptype = p.get("patch_type", "")
        if ptype in ("update_column_description", "update_description", "add_column_synonym"):
            tbl = (p.get("table") or p.get("target_table") or "").lower()
            col = (p.get("column") or p.get("target_column") or "").lower()
            proposed = re.sub(r"\s+", " ", (p.get("proposed_value") or "").strip().lower())
            short_tbl = tbl.rsplit(".", 1)[-1] if "." in tbl else tbl
            current = existing_descs.get((tbl, col)) or existing_descs.get((short_tbl, col)) or ""
            if current and proposed and _ngram_similarity(current, proposed) > 0.97:
                dropped += 1
                continue
        if ptype == "add_example_sql":
            eq = (p.get("example_question") or "").lower().strip()
            if eq and (
                eq in existing_eq_questions
                or any(_ngram_similarity(eq, existing) > _EXAMPLE_SQL_SIMILARITY_THRESHOLD
                       for existing in existing_eq_questions)
            ):
                logger.info("Filtering no-op/near-duplicate add_example_sql: %.80s", eq)
                dropped += 1
                continue
        kept.append(p)

    if dropped:
        logger.info(
            "Filtered %d no-op proposals (description unchanged from current metadata)",
            dropped,
        )
    return kept


def _deduplicate_proposals(proposals: list[dict]) -> list[dict]:
    """Remove duplicate proposals using type-aware deduplication.

    - ``update_column_description`` / ``add_column_synonym``: dedup by (table, column).
    - ``add_join_spec``: dedup by sorted (left_table, right_table) pair.
    - ``add_instruction``: merge near-duplicates (ngram similarity > 0.7).
    - ``add_example_sql``: dedup by normalized SQL text.
    - Others: dedup by exact (patch_type, proposed_value).
    """
    out: list[dict] = []

    col_desc_best: dict[tuple[str, str], int] = {}
    join_spec_best: dict[tuple[str, str], int] = {}
    instruction_entries: list[tuple[int, dict]] = []
    example_sql_seen: dict[str, int] = {}
    exact_seen: dict[tuple[str, str], int] = {}

    for p in proposals:
        ptype = p.get("patch_type", "")
        val = (p.get("proposed_value") or "").strip()
        impact = p.get("net_impact", 0)

        if ptype in ("update_column_description", "update_description", "add_column_synonym"):
            tbl = p.get("table", p.get("target_table", ""))
            col = p.get("column", p.get("target_column", ""))
            key = (tbl, col)
            if key in col_desc_best:
                existing_idx = col_desc_best[key]
                if impact > out[existing_idx].get("net_impact", 0):
                    out[existing_idx] = p
                continue
            col_desc_best[key] = len(out)
            out.append(p)

        elif ptype == "add_join_spec":
            js = p.get("join_spec", {})
            left_obj = js.get("left", {})
            right_obj = js.get("right", {})
            lt = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
            rt = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""
            if lt and rt:
                key = tuple(sorted((lt, rt)))
                if key in join_spec_best:
                    existing_idx = join_spec_best[key]
                    if impact > out[existing_idx].get("net_impact", 0):
                        out[existing_idx] = p
                    continue
                join_spec_best[key] = len(out)
            out.append(p)

        elif ptype == "add_instruction" and val:
            merged = False
            for existing_idx, existing_p in instruction_entries:
                existing_val = (existing_p.get("proposed_value") or "").strip()
                if _ngram_similarity(val, existing_val) > 0.7:
                    if impact > out[existing_idx].get("net_impact", 0):
                        out[existing_idx] = p
                        instruction_entries = [
                            (ei, ep) if ei != existing_idx else (ei, p)
                            for ei, ep in instruction_entries
                        ]
                    merged = True
                    break
            if not merged:
                instruction_entries.append((len(out), p))
                out.append(p)

        elif ptype == "add_example_sql" and val:
            normalized = re.sub(r"\s+", " ", val.lower()).strip()
            if normalized in example_sql_seen:
                existing_idx = example_sql_seen[normalized]
                if impact > out[existing_idx].get("net_impact", 0):
                    out[existing_idx] = p
                continue
            fuzzy_match_idx = None
            for seen_norm, seen_idx in example_sql_seen.items():
                if _ngram_similarity(normalized, seen_norm) > _EXAMPLE_SQL_SIMILARITY_THRESHOLD:
                    fuzzy_match_idx = seen_idx
                    break
            if fuzzy_match_idx is not None:
                if impact > out[fuzzy_match_idx].get("net_impact", 0):
                    out[fuzzy_match_idx] = p
                continue
            example_sql_seen[normalized] = len(out)
            out.append(p)

        else:
            key = (ptype, val)
            if key in exact_seen:
                existing_idx = exact_seen[key]
                if impact > out[existing_idx].get("net_impact", 0):
                    out[existing_idx] = p
                continue
            exact_seen[key] = len(out)
            out.append(p)

    return out


def _merge_overlapping_instructions(proposals: list[dict]) -> list[dict]:
    """Merge ``add_instruction`` proposals that share >70% keyword overlap.

    Keeps all non-instruction proposals intact. For instructions, groups those
    with high overlap and concatenates into a single combined instruction.
    """
    instructions: list[dict] = []
    others: list[dict] = []
    for p in proposals:
        if p.get("patch_type") == "add_instruction":
            instructions.append(p)
        else:
            others.append(p)

    if len(instructions) <= 1:
        return proposals

    def _keywords(text: str) -> set[str]:
        return {w.lower() for w in re.findall(r"\b\w{3,}\b", text)}

    merged: list[dict] = []
    used: set[int] = set()

    for i, p1 in enumerate(instructions):
        if i in used:
            continue
        group = [p1]
        kw1 = _keywords(p1.get("proposed_value", ""))
        for j, p2 in enumerate(instructions):
            if j <= i or j in used:
                continue
            kw2 = _keywords(p2.get("proposed_value", ""))
            if kw1 and kw2:
                overlap = len(kw1 & kw2) / len(kw1 | kw2)
                if overlap > 0.7:
                    group.append(p2)
                    used.add(j)
        used.add(i)

        if len(group) == 1:
            merged.append(group[0])
        else:
            best = max(group, key=lambda g: g.get("net_impact", 0))
            total_questions = sum(g.get("questions_fixed", 0) for g in group)
            best = dict(best)
            best["questions_fixed"] = total_questions
            best["merged_count"] = len(group)
            merged.append(best)
            logger.info(
                "Merged %d overlapping instructions into one (kept best net_impact=%.2f)",
                len(group), best.get("net_impact", 0),
            )

    return others + merged


_LEVER_NAMES = {0: "Proactive Enrichment", 1: "Tables & Columns", 2: "Metric Views", 3: "Table-Valued Functions", 4: "Join Specifications", 5: "Genie Space Instructions", 6: "SQL Expressions"}


def _build_provenance(cluster: dict, lever: int, patch_type: str) -> dict:
    """Build a provenance dict from a cluster's question_traces."""
    return {
        "cluster_id": cluster.get("cluster_id", ""),
        "root_cause": cluster.get("root_cause", "other"),
        "originating_questions": cluster.get("question_traces", []),
        "lever": lever,
        "lever_name": _LEVER_NAMES.get(lever, f"Lever {lever}"),
        "patch_type": patch_type,
    }


def _resolve_source_cluster_for_ag(
    action_group: dict, metadata_snapshot: dict,
) -> dict | None:
    """Return the first archetype-eligible source cluster for an action group.

    Used by the Lever 5 cluster-driven synthesis intercept. Action groups
    can span multiple clusters; synthesis runs once per AG so we pick the
    first cluster in ``source_cluster_ids`` whose ``root_cause`` maps to a
    shipped archetype (via :func:`archetypes.pick_archetype`). Clusters
    whose root_cause is terminology / data-quality / other non-SQL-shape
    return ``None`` so the caller falls back to text instructions rather
    than forcing the structural gate to reject every synthesized SQL.

    Returns ``None`` when no source cluster is archetype-eligible.
    """
    from genie_space_optimizer.optimization.afs import format_afs
    from genie_space_optimizer.optimization.archetypes import pick_archetype

    source_ids = action_group.get("source_cluster_ids", []) or []
    all_clusters = (
        metadata_snapshot.get("_failure_clusters")
        or metadata_snapshot.get("failure_clusters")
        or []
    )
    by_id = {
        c.get("cluster_id"): c
        for c in all_clusters if isinstance(c, dict)
    }
    for sid in source_ids:
        cluster = by_id.get(sid)
        if cluster is None:
            continue
        try:
            afs = format_afs(cluster)
        except Exception:
            logger.debug(
                "cluster-driven: format_afs failed for cluster=%s; skipping",
                sid, exc_info=True,
            )
            continue
        # Trial 17 step 7 — when GSO_TRIAL17_LEVER_LED_SYNTHESIS is on
        # (default), do not condition cluster selection on
        # archetype-pick success. Every candidate cluster proceeds to
        # Stage 3, which receives the archetype catalog as menu context.
        from genie_space_optimizer.optimization.trial17_flags import (
            trial17_lever_led_synthesis_enabled,
        )
        if trial17_lever_led_synthesis_enabled():
            return cluster
        if pick_archetype(afs, metadata_snapshot) is not None:
            return cluster
    return None


def _prune_doa_fingerprints(
    proposals: list[dict],
    *,
    buffer: Any,  # DoaFingerprintBuffer | None
    ag_id: str,
) -> list[dict]:
    """Cycle 9 W4 — drop any candidate whose retry signature was
    already captured as DOA in this run.

    End-of-function prune (rather than per-append filtering) is
    dramatically simpler and behaviourally equivalent because no
    caller reads the partial proposal list mid-function. No-op when
    ``buffer is None`` or when the
    ``GSO_DOA_FINGERPRINT_BLOCK_REPROPOSAL`` flag is off, so the
    flag-default-off path is byte-stable with the legacy code.
    """
    from genie_space_optimizer.common.config import (
        doa_fingerprint_block_reproposal_enabled,
    )
    if buffer is None or not doa_fingerprint_block_reproposal_enabled():
        return proposals
    try:
        return [
            p for p in (proposals or [])
            if not buffer.contains(ag_id=str(ag_id), patch=p)
        ]
    except Exception:
        logger.debug(
            "Cycle 9 W4: DOA fingerprint prune failed (non-fatal)",
            exc_info=True,
        )
        return proposals


def generate_proposals_from_strategy(
    strategy: dict,
    action_group: dict,
    metadata_snapshot: dict,
    target_lever: int,
    apply_mode: str = APPLY_MODE,
    w: WorkspaceClient | None = None,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
    doa_fingerprint_buffer: Any = None,
    # Plan 9 Task 4 — thread Plan-5 typed inputs from the harness so
    # the Plan-5 LLM intent short-circuit at _dispatch_lever_5b_for_cluster
    # (optimizer.py:10422) and at _generate_lever6_proposal
    # (optimizer.py:14122) actually fires on the live path.
    rca_evidence_typed: dict | None = None,
    llm_cluster_by_cluster_id: dict | None = None,
    ag_id: str | None = None,
    iteration: int = 0,
    run_id: str | None = None,
) -> list[dict]:
    """Generate proposals for a single lever guided by the holistic strategy.

    Each lever acts as an *executor*: it receives the strategist's directives
    for its action group and generates the concrete patch proposals accordingly.
    """
    import mlflow

    proposals: list[dict] = []
    ag_id = action_group.get("id", "AG?")
    directives = action_group.get("lever_directives", {})
    lever_key = str(target_lever)
    lever_dir = directives.get(lever_key, {})

    # Tier 2.7: receive any re-routed sections from sibling levers that
    # stashed them on the AG. Merge into this lever's directive so they
    # land in this iteration, not deferred to a future one.
    _pending_routing = action_group.get("_pending_section_routing") or {}
    _my_pending = _pending_routing.get(target_lever) or _pending_routing.get(lever_key)
    if isinstance(_my_pending, dict) and _my_pending:
        _existing = lever_dir.get("instruction_sections") or {}
        if not isinstance(_existing, dict):
            _existing = {}
        _merged = dict(_existing)
        for _sec, _val in _my_pending.items():
            _merged[_sec] = (
                (_merged.get(_sec, "") + "\n" + _val).strip()
                if _merged.get(_sec) else _val
            )
        lever_dir["instruction_sections"] = _merged
        logger.info(
            "Lever %d: absorbed %d re-routed section(s) from sibling levers: %s",
            target_lever, len(_my_pending), sorted(_my_pending.keys()),
        )

    _rca_execution = action_group.get("_rca_execution") or {}
    _rca_forces_lever = (
        isinstance(_rca_execution, dict)
        and int(target_lever) in {
            int(x) for x in (_rca_execution.get("required_levers") or [])
            if str(x).isdigit()
        }
    )

    if not lever_dir and target_lever not in (1, 4, 5, 6) and not _rca_forces_lever:
        return proposals

    scope = _resolve_scope(target_lever, apply_mode)
    coordination_notes = action_group.get("coordination_notes", "")
    root_cause = action_group.get("root_cause_summary", "")
    affected_qs = action_group.get("affected_questions", [])
    q_fixed = len(affected_qs)
    source_clusters = action_group.get("source_cluster_ids", [])

    # Phase 4.2: per-proposal rationale helper. Until this PR every
    # proposal stamped the same ``Strategy: <root_cause>.
    # <coordination_notes>`` string regardless of which target/section
    # was being modified — so the iter-1 log showed 18 proposals with
    # byte-identical rationale text. Append target identity so each
    # proposal carries its own context.
    def _per_target_rationale(
        target_id: str,
        section_keys: list[str] | None = None,
        extra: str = "",
    ) -> str:
        head = f"Strategy: {root_cause}." if root_cause else "Strategy:"
        parts: list[str] = [head]
        if coordination_notes:
            parts.append(coordination_notes.strip())
        if target_id:
            parts.append(f"Target: {target_id} (lever {target_lever}).")
        if section_keys:
            parts.append(f"Sections: {', '.join(sorted(section_keys))}.")
        if extra:
            parts.append(extra.strip())
        return " ".join(p for p in parts if p)

    provenance_base = {
        "cluster_id": ag_id,
        "root_cause": root_cause,
        "originating_questions": [],
        "lever": target_lever,
        "lever_name": _LEVER_NAMES.get(target_lever, f"Lever {target_lever}"),
        "patch_type": "",
    }

    _rca_bridge_target_qids = _target_qids_for_rca_bridges(
        action_group,
        strategy,
        metadata_snapshot,
    )

    def _rca_forced_instruction_proposal() -> dict | None:
        """Deterministic RCA bridge for forced Lever 5 with no directive.

        When the strategist routed the AG to Lever 5 via the RCA execution
        contract but did not emit a native ``instruction_sections`` directive,
        produce a causal instruction patch from the RCA grounding terms so the
        forced lever can still ground. Dispatch keyed on structured
        ``root_cause`` rather than substring search across grounding terms.
        """
        if target_lever != 5 or not _rca_forces_lever:
            return None
        terms = [
            str(t).strip()
            for t in (_rca_execution.get("grounding_terms") or [])
            if str(t).strip()
        ]
        cluster_root_cause = str(
            _rca_execution.get("root_cause")
            or (action_group or {}).get("root_cause")
            or ""
        ).strip()
        question_text = str(
            (action_group or {}).get("representative_question") or ""
        )
        expected_sql_text = str(
            (action_group or {}).get("representative_expected_sql") or ""
        )
        body = _build_rca_forced_instruction_body(
            root_cause=cluster_root_cause,
            grounding_terms=terms,
            question=question_text,
            expected_sql=expected_sql_text,
        )
        if body is None:
            return None
        patch_type = (
            "add_example_sql"
            if cluster_root_cause == "format_difference"
            else "add_instruction"
        )
        return {
            "proposal_id": f"P{len(proposals) + 1:03d}",
            "cluster_id": ag_id,
            "lever": 5,
            "scope": "genie_config",
            "patch_type": patch_type,
            "change_description": f"[{ag_id}] RCA forced bridge ({cluster_root_cause})",
            "proposed_value": body,
            "rationale": _per_target_rationale(
                "rca_forced_instruction",
                extra=f"deterministic RCA bridge for {cluster_root_cause}",
            ),
            "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
            "confidence": 0.75,
            "questions_fixed": q_fixed,
            "questions_at_risk": 0,
            "net_impact": max(q_fixed * 0.75, 1.0),
            "asi": {
                "failure_type": root_cause or "rca_forced_instruction",
                "blame_set": terms,
                "severity": "major",
                "counterfactual_fixes": [],
                "ambiguity_detected": False,
            },
            "rca_id": ",".join(str(x) for x in (_rca_execution.get("rca_ids") or [])),
            "patch_family": ",".join(
                str(x) for x in (_rca_execution.get("defect_keys") or [])
            ),
            "target_qids": affected_qs,
            "_rca_grounding_terms": terms,
            "provenance": {**provenance_base, "patch_type": patch_type},
        }

    from mlflow.entities import SpanType as _SpanType

    with mlflow.start_span(
        name=f"generate_proposals_lever_{target_lever}_ag_{ag_id}",
        span_type=_SpanType.CHAIN,
    ) as span:
        span.set_inputs({
            "action_group_id": ag_id,
            "target_lever": target_lever,
            "root_cause": root_cause[:200],
            "affected_questions": len(affected_qs),
            "directives_keys": list(lever_dir.keys()) if isinstance(lever_dir, dict) else [],
        })

        # ── Lever 1 / 2: table + column metadata ────────────────────────
        if target_lever in (1, 2):
            for tbl_entry in lever_dir.get("tables", []):
                if not isinstance(tbl_entry, dict):
                    continue
                tbl = tbl_entry.get("table", "")
                tbl_sections = tbl_entry.get("sections", {})
                tbl_etype = tbl_entry.get("entity_type", "table")
                if tbl and isinstance(tbl_sections, dict) and tbl_sections:
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": ag_id,
                        "lever": target_lever,
                        "scope": scope,
                        "patch_type": "update_description",
                        "change_description": f"[{ag_id}] Update table {tbl} sections={list(tbl_sections.keys())}",
                        "proposed_value": "",
                        "rationale": _per_target_rationale(
                            tbl, list(tbl_sections.keys()),
                        ),
                        "dual_persistence": DUAL_PERSIST_PATHS.get(target_lever, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.85,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": max(q_fixed * 0.85, 1.0),
                        "asi": {
                            "failure_type": root_cause,
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "update_description"},
                        "table": tbl,
                        "table_sections": tbl_sections,
                        "table_entity_type": tbl_etype,
                    })

            for col_entry in lever_dir.get("columns", []):
                if not isinstance(col_entry, dict):
                    continue
                tbl = col_entry.get("table", "")
                col = col_entry.get("column", "")
                col_sections = col_entry.get("sections", {})
                col_etype = col_entry.get("entity_type", "")
                if tbl and col and isinstance(col_sections, dict) and col_sections:
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": ag_id,
                        "lever": target_lever,
                        "scope": scope,
                        "patch_type": "update_column_description",
                        "change_description": f"[{ag_id}] Update {tbl}.{col} sections={list(col_sections.keys())}",
                        "proposed_value": "",
                        "rationale": _per_target_rationale(
                            f"{tbl}.{col}", list(col_sections.keys()),
                        ),
                        "dual_persistence": DUAL_PERSIST_PATHS.get(target_lever, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.85,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": max(q_fixed * 0.85, 1.0),
                        "asi": {
                            "failure_type": root_cause,
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "update_column_description"},
                        "table": tbl,
                        "column": col,
                        "column_sections": col_sections,
                        "column_entity_type": col_etype,
                        "metric_view_columns": _select_metric_view_columns(
                            col_entry, action_group,
                        ),
                    })

            # Plan 8 Task 10 — lever-1-rca-bridge dispatch retired;
            # absorbed into lever-1-table-column-description (intent-aware).

        # ── Lever 3: functions ───────────────────────────────────────────
        elif target_lever == 3:
            for fn_entry in lever_dir.get("functions", []):
                if not isinstance(fn_entry, dict):
                    continue
                fn_name = fn_entry.get("function", "")
                fn_sections = fn_entry.get("sections", {})
                if fn_name and isinstance(fn_sections, dict) and fn_sections:
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": ag_id,
                        "lever": 3,
                        "scope": scope,
                        "patch_type": "update_function_description",
                        "change_description": f"[{ag_id}] Update function {fn_name} sections={list(fn_sections.keys())}",
                        "proposed_value": "",
                        "rationale": _per_target_rationale(
                            fn_name, list(fn_sections.keys()),
                        ),
                        "dual_persistence": DUAL_PERSIST_PATHS.get(3, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.85,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": max(q_fixed * 0.85, 1.0),
                        "asi": {
                            "failure_type": root_cause,
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "update_function_description"},
                        "function": fn_name,
                        "function_sections": fn_sections,
                    })

        # ── Lever 4: join specs ──────────────────────────────────────────
        elif target_lever == 4:
            _inst_l4 = metadata_snapshot.get("instructions", {})
            if not isinstance(_inst_l4, dict):
                _inst_l4 = {}
            _existing_join_specs = _inst_l4.get("join_specs", [])
            if not isinstance(_existing_join_specs, list):
                _existing_join_specs = []
            _existing_join_pairs: set[tuple[str, str]] = set()
            for _ejs in _existing_join_specs:
                if not isinstance(_ejs, dict):
                    continue
                _ej_left = _ejs.get("left", {})
                _ej_right = _ejs.get("right", {})
                _ej_lt = _ej_left.get("identifier", "") if isinstance(_ej_left, dict) else ""
                _ej_rt = _ej_right.get("identifier", "") if isinstance(_ej_right, dict) else ""
                if _ej_lt and _ej_rt:
                    _existing_join_pairs.add(tuple(sorted((_ej_lt, _ej_rt))))

            for js_entry in lever_dir.get("join_specs", []):
                if not isinstance(js_entry, dict):
                    continue
                left_table = js_entry.get("left_table", "")
                right_table = js_entry.get("right_table", "")
                guidance = js_entry.get("join_guidance", "")
                if left_table and right_table:
                    if tuple(sorted((left_table, right_table))) in _existing_join_pairs:
                        logger.info("[%s] Join spec skipped (already defined): %s ↔ %s", ag_id, left_table, right_table)
                        continue
                    sanitized_guidance = _sanitize_join_sql(guidance) if guidance else ""
                    join_spec = ensure_join_spec_fields({
                        "left": {"identifier": left_table},
                        "right": {"identifier": right_table},
                        "sql": [sanitized_guidance] if sanitized_guidance else [],
                    }, config=metadata_snapshot)
                    valid, reason = validate_join_spec_types(join_spec, metadata_snapshot)
                    if not valid:
                        logger.info("[%s] Join spec rejected (type mismatch): %s", ag_id, reason)
                        continue
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": ag_id,
                        "lever": 4,
                        "scope": "genie_config",
                        "patch_type": "add_join_spec",
                        "change_description": f"[{ag_id}] Join: {left_table} ↔ {right_table}",
                        "proposed_value": "",
                        "rationale": _per_target_rationale(
                            f"{left_table} <-> {right_table}",
                            extra="add_join_spec",
                        ),
                        "join_spec": join_spec,
                        "dual_persistence": DUAL_PERSIST_PATHS.get(4, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.8,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": max(q_fixed * 0.8, 1.0),
                        "asi": {
                            "failure_type": root_cause,
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "add_join_spec"},
                    })

            # Fallback: use judge-provided join_assessments from source clusters
            _all_clusters = strategy.get("_source_clusters", [])
            _ja_entries: list[dict] = []
            for _sc_id in source_clusters:
                for _clust in _all_clusters:
                    if _clust.get("cluster_id") == _sc_id:
                        _ja_entries.extend(_clust.get("join_assessments", []))
            _proposed_pairs = {
                tuple(sorted((p["join_spec"]["left"]["identifier"],
                               p["join_spec"]["right"]["identifier"])))
                for p in proposals if p.get("join_spec")
            } | _existing_join_pairs
            for _ja in _ja_entries:
                _lt = _ja.get("left_table", "")
                _rt = _ja.get("right_table", "")
                if not _lt or not _rt:
                    continue
                _pair = tuple(sorted((_lt, _rt)))
                if _pair in _proposed_pairs:
                    continue
                _cond = _ja.get("suggested_condition", "")
                _sanitized = _sanitize_join_sql(_cond) if _cond else ""
                _j_spec = ensure_join_spec_fields({
                    "left": {"identifier": _lt},
                    "right": {"identifier": _rt},
                    "sql": [_sanitized] if _sanitized else [],
                }, config=metadata_snapshot)
                _valid, _reason = validate_join_spec_types(_j_spec, metadata_snapshot)
                if not _valid:
                    logger.info("[%s] Judge join_assessment rejected (type): %s", ag_id, _reason)
                    continue
                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": f"{ag_id}_JA",
                    "lever": 4,
                    "scope": "genie_config",
                    "patch_type": "add_join_spec",
                    "change_description": f"[{ag_id}] Judge-assessed join: {_lt} ↔ {_rt}",
                    "proposed_value": "",
                    "rationale": f"Judge assessment: {_ja.get('evidence', '')}",
                    "join_spec": _j_spec,
                    "dual_persistence": DUAL_PERSIST_PATHS.get(4, DUAL_PERSIST_PATHS[5]),
                    "confidence": 0.75,
                    "questions_fixed": q_fixed,
                    "questions_at_risk": 0,
                    "net_impact": max(q_fixed * 0.75, 1.0),
                    "asi": {
                        "failure_type": "missing_join_spec",
                        "blame_set": [_lt, _rt],
                        "severity": "major",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    },
                    "provenance": {**provenance_base, "patch_type": "add_join_spec"},
                })
                _proposed_pairs.add(_pair)

            discovery_hints = discover_join_candidates(metadata_snapshot)
            if discovery_hints:
                discovery_specs = _call_llm_for_join_discovery(
                    metadata_snapshot, discovery_hints, w=w,
                )
                for spec_result in discovery_specs:
                    join_spec = spec_result.get("join_spec")
                    if not isinstance(join_spec, dict):
                        continue
                    join_spec = ensure_join_spec_fields(join_spec, config=metadata_snapshot)
                    spec_result["join_spec"] = join_spec
                    valid, reason = validate_join_spec_types(join_spec, metadata_snapshot)
                    if not valid:
                        logger.info("[%s] Discovery join rejected: %s", ag_id, reason)
                        continue
                    left_obj = join_spec.get("left", {})
                    right_obj = join_spec.get("right", {})
                    left_id = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
                    right_id = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""
                    sql_parts = join_spec.get("sql", [])
                    condition = sql_parts[0] if sql_parts else ""
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": f"{ag_id}_DISC_{len(proposals) + 1:03d}",
                        "lever": 4,
                        "scope": "genie_config",
                        "patch_type": "add_join_spec",
                        "change_description": f"[{ag_id}] Discover join: {condition}" if condition else f"[{ag_id}] Join: {left_id} ↔ {right_id}",
                        "proposed_value": "",
                        "rationale": spec_result.get("rationale", "LLM-assisted join discovery"),
                        "join_spec": join_spec,
                        "dual_persistence": DUAL_PERSIST_PATHS.get(4, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.7,
                        "questions_fixed": 0,
                        "questions_at_risk": 0,
                        "net_impact": 0.35,
                        "asi": {
                            "failure_type": "missing_join_spec",
                            "blame_set": [],
                            "severity": "minor",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                    })

            if ENABLE_RCA_JOIN_SPEC_BRIDGE:
                _bridged_themes = _rca_themes_requesting_join_specs(
                    metadata_snapshot.get("_rca_themes") or [],
                    target_qids=_rca_bridge_target_qids,
                )
                for _theme in _bridged_themes:
                    _theme_patches = list(getattr(_theme, "patches", ()) or ())
                    _join_patches = [
                        p for p in _theme_patches
                        if isinstance(p, dict) and p.get("type") == "add_join_spec"
                    ]
                    if not _join_patches:
                        continue
                    _theme_id = str(getattr(_theme, "rca_id", ag_id))
                    _theme_qids = list(getattr(_theme, "target_qids", ()) or ())
                    _theme_family = str(getattr(_theme, "patch_family", ""))
                    for _p in _join_patches:
                        _expected_objects = [
                            str(o).strip()
                            for o in (_p.get("expected_objects") or [])
                            if str(o).strip() and "." in str(o)
                        ]
                        if len(_expected_objects) < 2:
                            logger.debug(
                                "[%s] RCA join bridge: theme %s has %d qualified "
                                "expected_objects (need 2); skipping",
                                ag_id, _theme_id, len(_expected_objects),
                            )
                            continue
                        _left_obj, _right_obj = _expected_objects[0], _expected_objects[1]
                        _left_table, _left_col = _left_obj.rsplit(".", 1)
                        _right_table, _right_col = _right_obj.rsplit(".", 1)
                        if _left_table == _right_table:
                            logger.debug(
                                "[%s] RCA join bridge: same-table pair %s; skipping",
                                ag_id, _left_table,
                            )
                            continue
                        _pair = tuple(sorted((_left_table, _right_table)))
                        if _pair in _proposed_pairs:
                            logger.info(
                                "[%s] RCA join bridge: pair %s already proposed; skipping",
                                ag_id, _pair,
                            )
                            continue
                        _condition = f"{_left_obj} = {_right_obj}"
                        _sanitized = _sanitize_join_sql(_condition)
                        _join_spec = ensure_join_spec_fields({
                            "left": {"identifier": _left_table},
                            "right": {"identifier": _right_table},
                            "sql": [_sanitized] if _sanitized else [],
                        }, config=metadata_snapshot)
                        _valid, _reason = validate_join_spec_types(
                            _join_spec, metadata_snapshot,
                        )
                        if not _valid:
                            logger.info(
                                "[%s] RCA join bridge rejected (type): %s",
                                ag_id, _reason,
                            )
                            continue
                        proposals.append({
                            "proposal_id": f"P{len(proposals) + 1:03d}",
                            "cluster_id": _theme_id,
                            "lever": 4,
                            "scope": "genie_config",
                            "patch_type": "add_join_spec",
                            "change_description": (
                                f"[{ag_id}] RCA join: {_left_table} ↔ {_right_table}"
                            ),
                            "proposed_value": "",
                            "rationale": (
                                f"RCA-driven join from theme {_theme_id} "
                                f"({_p.get('intent', 'derived from expected_sql')})"
                            ),
                            "join_spec": _join_spec,
                            "dual_persistence": DUAL_PERSIST_PATHS.get(
                                4, DUAL_PERSIST_PATHS[5],
                            ),
                            "confidence": 0.78,
                            "questions_fixed": len(_theme_qids),
                            "questions_at_risk": 0,
                            "net_impact": max(len(_theme_qids) * 0.78, 1.0),
                            "asi": {
                                "failure_type": "missing_join_spec",
                                "blame_set": [_left_table, _right_table],
                                "severity": "major",
                                "counterfactual_fixes": [],
                                "ambiguity_detected": False,
                            },
                            "rca_id": _theme_id,
                            "patch_family": _theme_family,
                            "target_qids": _theme_qids,
                            "source": "rca_theme_lever4",
                            "provenance": {
                                **provenance_base,
                                "patch_type": "add_join_spec",
                                "synthesis_source": "rca_theme_lever4",
                                "rca_id": _theme_id,
                                "patch_family": _theme_family,
                            },
                        })
                        _proposed_pairs.add(_pair)

        # ── Lever 5: instructions + example SQL ──────────────────────────
        elif target_lever == 5:
            l5_dir = lever_dir or {}
            if not lever_dir:
                bridge = _rca_forced_instruction_proposal()
                if bridge:
                    proposals.append(bridge)
            instruction_sections = l5_dir.get("instruction_sections")
            instruction_guidance = (l5_dir.get("instruction_guidance") or "").strip()

            example_sqls_list = l5_dir.get("example_sqls", [])
            if not example_sqls_list:
                legacy = l5_dir.get("example_sql")
                if isinstance(legacy, dict):
                    example_sqls_list = [legacy]
            if not isinstance(example_sqls_list, list):
                example_sqls_list = [example_sqls_list] if isinstance(example_sqls_list, dict) else []

            # ── Phase A3a: Lever 5 structural gate ──────────────────────
            # For clusters whose dominant root cause is SQL-shape
            # (missing_filter, wrong_aggregation, wrong_join, etc.), a
            # text-only instruction is a weak signal. Require an
            # example_sql; otherwise drop the instruction path entirely
            # and let the example_sqls_list path (cluster-driven
            # synthesis) carry the fix. This prevents Q004-class
            # mis-diagnoses where the strategist's counterfactual came
            # from the NL-text judge but the failure is structural.
            _failure_clusters = (
                metadata_snapshot.get("_failure_clusters")
                or metadata_snapshot.get("failure_clusters")
                or []
            )
            _cluster_by_id = {
                c.get("cluster_id"): c
                for c in _failure_clusters if isinstance(c, dict)
            }
            # Phase 6.3 (2026-05-17) — wire the existing
            # ``cluster_failure_keys`` helper so the L5 structural
            # gate evaluates BOTH ``asi_failure_type`` and
            # ``root_cause``, not just the first truthy one. The
            # pre-Phase-6 ``or`` short-circuit missed SQL-shape
            # ``root_cause`` whenever ``asi_failure_type`` was a
            # non-SQL-shape label like ``"other"``.
            _ag_structural_root_causes: set[str] = (
                _ag_structural_root_causes_for_clusters(
                    source_clusters=source_clusters,
                    clusters_by_id=_cluster_by_id,
                )
            )

            _l5_structural_gate_blocked = bool(
                _ag_structural_root_causes and not example_sqls_list
            )
            if _l5_structural_gate_blocked:
                _incr_bug4_counter("lever5_text_only_blocked")
                # Cycle 8 Bug 1 Phase 3b Task B: capture the drop on a
                # side-channel ledger so the harness can build a typed
                # GATE_DECISION DecisionRecord for the operator
                # transcript. The instruction silencing below remains
                # the active behaviour; this is observability only.
                _LEVER5_GATE_DROPS.append({
                    "ag_id": str(ag_id),
                    "source_clusters": tuple(str(s) for s in source_clusters),
                    "root_causes": tuple(sorted(_ag_structural_root_causes)),
                    "target_lever": 5,
                    "had_example_sqls": bool(example_sqls_list),
                    "instruction_sections_dropped": (
                        isinstance(instruction_sections, dict)
                        and bool(instruction_sections)
                    ),
                    "instruction_guidance_dropped": bool(instruction_guidance),
                })
                logger.warning(
                    "[%s] Lever 5 structural gate: dropping instruction-only "
                    "proposal. Dominant cluster root cause(s) %s are SQL-shape; "
                    "no example_sql attached. Expected structural fix via "
                    "cluster-driven synthesis or a different lever.",
                    ag_id, sorted(_ag_structural_root_causes),
                )
                instruction_sections = None
                instruction_guidance = ""

            # Computed once for the whole Lever 5 block so both the
            # instruction_sections branch and the instruction_guidance
            # branch can reference it. Previously assigned only inside
            # the if-branch, which made the elif raise UnboundLocalError
            # whenever the strategist emitted free-form instruction
            # guidance with no structured sections.
            invoked_levers = {
                int(k) for k in action_group.get("lever_directives", {}).keys()
                if str(k).isdigit()
            }

            if isinstance(instruction_sections, dict) and instruction_sections:
                from genie_space_optimizer.optimization.applier import _get_general_instructions

                valid_keys = set(INSTRUCTION_SECTION_ORDER)
                invalid = [k for k in instruction_sections if k not in valid_keys]
                if invalid:
                    logger.warning("Ignoring unknown instruction sections: %s", invalid)
                    instruction_sections = {
                        k: v for k, v in instruction_sections.items() if k in valid_keys
                    }

                # --- Task 4 / Tier 2.7: section ownership enforcement ---
                # Before folding unauthorised sections into CONSTRAINTS,
                # check whether another invoked lever in this AG owns the
                # section via LEVER_TO_SECTIONS. If so, stash the section
                # on the AG as ``_pending_section_routing`` so the other
                # lever's proposal generator can pick it up. If no
                # invoked lever owns it, DROP the section rather than
                # polluting CONSTRAINTS with semantically-different
                # content (JOIN GUIDANCE, TEMPORAL FILTERS, etc.). This
                # fixes the observed AG2 collapse where both JOIN
                # GUIDANCE (Lever 4) and TEMPORAL FILTERS (Lever 2) were
                # dumped into Lever 5's CONSTRAINTS.
                allowed_sections = set(LEVER_TO_SECTIONS.get(target_lever, []))
                if allowed_sections:
                    unauthorized = {
                        k for k in instruction_sections if k not in allowed_sections
                    }
                    if unauthorized:
                        _rerouted: dict[int, dict[str, str]] = {}
                        _dropped: list[str] = []
                        for k in sorted(unauthorized):
                            val = instruction_sections.pop(k, "")
                            if not val:
                                continue
                            # Find another invoked lever that owns this section.
                            _owner_lever = None
                            for _lv in sorted(invoked_levers):
                                if _lv == target_lever:
                                    continue
                                if k in set(LEVER_TO_SECTIONS.get(_lv, [])):
                                    _owner_lever = _lv
                                    break
                            if _owner_lever is not None:
                                _rerouted.setdefault(_owner_lever, {})[k] = val
                            else:
                                _dropped.append(k)

                        if _rerouted:
                            # Stash on action_group so the owning lever's
                            # proposal generator can pick it up in the
                            # same iteration. Keyed by lever int.
                            _pending = action_group.setdefault(
                                "_pending_section_routing", {}
                            )
                            for _lv, _secs in _rerouted.items():
                                _pending.setdefault(_lv, {}).update(_secs)
                            logger.warning(
                                "Lever %d: re-routed %d section(s) to invoked "
                                "owner lever(s): %s",
                                target_lever,
                                sum(len(v) for v in _rerouted.values()),
                                {lv: sorted(secs.keys()) for lv, secs in _rerouted.items()},
                            )
                        if _dropped:
                            logger.warning(
                                "Lever %d: dropping %d unauthorised section(s) "
                                "with no invoked owner: %s (LEVER_TO_SECTIONS "
                                "maps to levers not in this AG; deferred to a "
                                "future iteration that invokes those levers)",
                                target_lever, len(_dropped), sorted(_dropped),
                            )

                # --- Contradiction check against user-authored instructions ---
                _orig_sections = metadata_snapshot.get("_original_instruction_sections")
                if _orig_sections and isinstance(_orig_sections, dict):
                    _contradictions = _detect_instruction_contradictions(
                        _orig_sections, instruction_sections,
                    )
                    for _c in _contradictions:
                        logger.warning(
                            "REJECTED contradictory instruction: section=%s "
                            "proposed='%s' contradicts original='%s' (%s)",
                            _c["section"],
                            _c["proposed_line"][:120],
                            _c["original_rule"][:120],
                            _c["contradiction_type"],
                        )
                        _c_section = _c["section"]
                        _c_line = _c["proposed_line"]
                        _sec_val = instruction_sections.get(_c_section, "")
                        if isinstance(_sec_val, str) and _c_line in _sec_val:
                            instruction_sections[_c_section] = _sec_val.replace(
                                _c_line, ""
                            ).strip()
                        elif isinstance(_sec_val, list):
                            instruction_sections[_c_section] = [
                                ln for ln in _sec_val if _c_line not in ln
                            ]
                    if _contradictions:
                        instruction_sections = {
                            k: v for k, v in instruction_sections.items()
                            if (isinstance(v, str) and v.strip()) or (isinstance(v, list) and v)
                        }
                        logger.info(
                            "Stripped %d contradictory line(s) from proposed instructions",
                            len(_contradictions),
                        )

                current_instructions = _get_general_instructions(metadata_snapshot)

                # Pre-structure existing instructions if unstructured
                existing_sections = _ensure_structured(
                    current_instructions, metadata_snapshot, w=w,
                )
                merged_secs: dict[str, list[str]] = {
                    s: list(existing_sections.get(s, []))
                    for s in INSTRUCTION_SECTION_ORDER
                }
                for key, value in instruction_sections.items():
                    if not isinstance(value, str):
                        continue
                    if value == "":
                        merged_secs[key] = []
                    else:
                        merged_secs[key] = [ln for ln in value.splitlines() if ln.strip()]

                parts: list[str] = []
                for section in INSTRUCTION_SECTION_ORDER:
                    lines = merged_secs[section]
                    if not lines:
                        continue
                    parts.append(f"{section}:")
                    for ln in lines:
                        s = ln.strip()
                        if not s:
                            continue
                        if not s.startswith("- "):
                            s = f"- {s}"
                        parts.append(s)
                    parts.append("")
                merged_text = _sanitize_plaintext_instructions("\n".join(parts).strip())

                if (
                    current_instructions
                    and merged_text
                    and len(current_instructions.strip()) > 50
                    and len(merged_text.strip()) < len(current_instructions.strip()) * _INSTR_LOSS_THRESHOLD
                ):
                    logger.warning(
                        "Instruction rewrite would lose content (%d -> %d chars) "
                        "— force-merging existing instructions",
                        len(current_instructions), len(merged_text),
                    )
                    merged_text = _merge_structured_instructions(
                        existing=current_instructions,
                        contributions=[merged_text],
                    )

                if current_instructions and not _instruction_coverage(
                    current_instructions, merged_text
                ):
                    logger.warning(
                        "Instruction rewrite drops key phrases — force-merging"
                    )
                    merged_text = _merge_structured_instructions(
                        existing=current_instructions,
                        contributions=[merged_text],
                    )

                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": ag_id,
                    "lever": 5,
                    "scope": "genie_config",
                    "patch_type": "rewrite_instruction",
                    "change_description": f"[{ag_id}] Instruction rewrite ({len(merged_text)} chars)",
                    "proposed_value": merged_text,
                    "old_value": current_instructions,
                    "rationale": _per_target_rationale(
                        "general_instructions",
                        extra=f"rewrite_instruction ({len(merged_text)} chars)",
                    ),
                    "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                    "confidence": 0.85,
                    "questions_fixed": q_fixed,
                    "questions_at_risk": 0,
                    "net_impact": max(q_fixed * 0.85, 1.0),
                    "asi": {
                        "failure_type": "missing_instruction",
                        "blame_set": source_clusters,
                        "severity": "major",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    },
                    "provenance": {**provenance_base, "patch_type": "rewrite_instruction"},
                    "invoked_levers": sorted(invoked_levers),
                })

            elif instruction_guidance:
                from genie_space_optimizer.optimization.applier import _get_general_instructions

                current_instructions = _get_general_instructions(metadata_snapshot)
                merged_text = _merge_structured_instructions(
                    existing=current_instructions,
                    contributions=[instruction_guidance],
                )
                if (
                    current_instructions
                    and merged_text
                    and len(current_instructions.strip()) > 50
                    and len(merged_text.strip()) < len(current_instructions.strip()) * _INSTR_LOSS_THRESHOLD
                ):
                    logger.warning(
                        "Instruction rewrite would lose content (%d -> %d chars) "
                        "— force-merging existing instructions",
                        len(current_instructions), len(merged_text),
                    )
                    merged_text = _merge_structured_instructions(
                        existing=current_instructions,
                        contributions=[merged_text],
                    )

                if current_instructions and not _instruction_coverage(
                    current_instructions, merged_text
                ):
                    logger.warning(
                        "Instruction guidance rewrite drops key phrases — force-merging"
                    )
                    merged_text = _merge_structured_instructions(
                        existing=current_instructions,
                        contributions=[merged_text],
                    )

                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": ag_id,
                    "lever": 5,
                    "scope": "genie_config",
                    "patch_type": "rewrite_instruction",
                    "change_description": f"[{ag_id}] Instruction rewrite ({len(merged_text)} chars)",
                    "proposed_value": merged_text,
                    "old_value": current_instructions,
                    "rationale": _per_target_rationale(
                        "general_instructions",
                        extra=f"instruction_guidance ({len(merged_text)} chars)",
                    ),
                    "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                    "confidence": 0.85,
                    "questions_fixed": q_fixed,
                    "questions_at_risk": 0,
                    "net_impact": max(q_fixed * 0.85, 1.0),
                    "asi": {
                        "failure_type": "missing_instruction",
                        "blame_set": source_clusters,
                        "severity": "major",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    },
                    "provenance": {**provenance_base, "patch_type": "rewrite_instruction"},
                    "invoked_levers": sorted(invoked_levers),
                })

            # ── Lever 5 example_sql — cluster-driven synthesis intercept ──
            # Bug #4 Phase 3. When the feature flag is ON (default),
            # each strategist-emitted example_sql request becomes a
            # synthesis attempt driven by the AFS of the action group's
            # source cluster. The strategist's (question, sql_sketch)
            # tuple is discarded — those fields were generated from a
            # prompt that saw raw benchmark text and are therefore
            # leak-risky. Synthesis uses AFS only (leak-free by
            # construction).
            #
            # When the flag is OFF, the legacy verbatim path runs —
            # reserved for emergency rollback.
            #
            # Invariants (see cluster_driven_synthesis module docstring):
            #   A. We return proposals here; we do NOT apply directly.
            #   B. space_id is read from metadata_snapshot["_space_id"].
            #   C. Budget counter is shared across AGs via
            #      metadata_snapshot["_cluster_synthesis_count"].
            #   D. Missing-join-spec fallback handled inside the
            #      cluster-driven module.
            from genie_space_optimizer.common.config import (
                ENABLE_CLUSTER_DRIVEN_SYNTHESIS,
            )

            for ex_idx, example_sql_dir in enumerate(example_sqls_list):
                if not isinstance(example_sql_dir, dict):
                    continue

                if not ENABLE_CLUSTER_DRIVEN_SYNTHESIS:
                    # Legacy path preserved behind kill-switch. Unchanged
                    # shape from before the Bug #4 Phase 3 intercept.
                    eq = (example_sql_dir.get("question") or "").strip()
                    es = (example_sql_dir.get("sql_sketch") or "").strip()
                    if eq and es:
                        proposals.append({
                            "proposal_id": f"P{len(proposals) + 1:03d}",
                            "cluster_id": f"{ag_id}_EX{ex_idx + 1}",
                            "lever": 5,
                            "scope": "genie_config",
                            "patch_type": "add_example_sql",
                            "change_description": f"[{ag_id}] Example SQL {ex_idx + 1}: {eq[:80]}",
                            "proposed_value": eq,
                            "example_question": eq,
                            "example_sql": es,
                            "parameters": example_sql_dir.get("parameters", []),
                            "usage_guidance": example_sql_dir.get("usage_guidance", ""),
                            "rationale": _per_target_rationale(
                                f"example_sql_{ex_idx + 1}",
                                extra=eq[:120],
                            ),
                            "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                            "confidence": 0.8,
                            "questions_fixed": 1,
                            "questions_at_risk": 0,
                            "net_impact": 0.8,
                            "asi": {
                                "failure_type": "asset_routing_error",
                                "blame_set": source_clusters,
                                "severity": "major",
                                "counterfactual_fixes": [],
                                "ambiguity_detected": False,
                            },
                            "provenance": {**provenance_base, "patch_type": "add_example_sql"},
                        })
                    continue

                # Cluster-driven path: discard strategist's fields,
                # synthesize fresh via AFS engine. Plan 11 routes to the
                # LLM-first diagnose/cluster/synthesize stages when
                # plan11_llm_first_enabled() — see the
                # _dispatch_plan11_synthesis_for_legacy_cluster helper
                # for the dict-cluster → FailureCluster adapter.
                from genie_space_optimizer.common.config import (
                    plan11_llm_first_enabled,
                )
                if plan11_llm_first_enabled():
                    _dispatch_synth = (
                        _dispatch_plan11_synthesis_for_legacy_cluster
                    )
                else:
                    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
                        run_cluster_driven_synthesis_for_single_cluster,
                    )
                    _dispatch_synth = (
                        run_cluster_driven_synthesis_for_single_cluster
                    )
                from genie_space_optimizer.optimization.afs import format_afs
                from genie_space_optimizer.optimization.synthesis import (
                    instruction_only_fallback,
                )

                source_cluster = _resolve_source_cluster_for_ag(
                    action_group, metadata_snapshot,
                )
                if source_cluster is None:
                    # No archetype-eligible source cluster — nothing to
                    # synthesize. Skip silently; strategist's intent is
                    # already represented elsewhere in the AG (e.g. text
                    # instruction sections).
                    logger.info(
                        "cluster-driven: AG %s has no archetype-eligible "
                        "source cluster — skipping example_sql %d",
                        ag_id, ex_idx + 1,
                    )
                    continue

                # P3 task 1: synthesis driver returns a typed
                # ClusterSynthesisResult instead of dict-or-None;
                # read .proposal to preserve the legacy dict-or-None
                # contract at this call site.
                _synth_result = _dispatch_synth(
                    source_cluster,
                    metadata_snapshot,
                    benchmarks=benchmarks,
                    catalog=catalog, gold_schema=gold_schema,
                    warehouse_id=warehouse_id,
                    w=w, spark=spark,
                )
                synth_proposal = _synth_result.proposal

                if synth_proposal is None:
                    # Synthesis or a gate rejected. Fall back to
                    # deterministic instruction-only proposal — safe
                    # under the firewall because it references only AFS
                    # summary fields.
                    fallback = instruction_only_fallback(
                        format_afs(source_cluster),
                    )
                    if fallback is None:
                        continue
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": f"{ag_id}_EX{ex_idx + 1}",
                        "lever": 5,
                        "scope": "genie_config",
                        "patch_type": "add_instruction",
                        "change_description": (
                            f"[{ag_id}] Instruction-only fallback "
                            "(cluster-driven synthesis declined)"
                        ),
                        "proposed_value": str(fallback.get("new_text", "")),
                        "new_text": str(fallback.get("new_text", "")),
                        "rationale": (
                            f"Cluster-driven synthesis for cluster "
                            f"{source_cluster.get('cluster_id', '?')} failed; "
                            "applying deterministic instruction fallback. "
                            f"Root cause: {root_cause}"
                        ),
                        "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.6,
                        "questions_fixed": 1,
                        "questions_at_risk": 0,
                        "net_impact": 0.5,
                        "asi": {
                            "failure_type": "asset_routing_error",
                            "blame_set": source_clusters,
                            "severity": "minor",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {
                            **provenance_base,
                            "patch_type": "add_instruction",
                            "synthesis_source": "cluster_driven_fallback",
                        },
                    })
                    continue

                # Synthesis succeeded — shape Lever 5 proposal.
                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": f"{ag_id}_EX{ex_idx + 1}",
                    "lever": 5,
                    "scope": "genie_config",
                    "patch_type": "add_example_sql",
                    "change_description": (
                        f"[{ag_id}] Synthesized example SQL: "
                        f"{synth_proposal['example_question'][:80]}"
                    ),
                    "proposed_value": synth_proposal["example_question"],
                    "example_question": synth_proposal["example_question"],
                    "example_sql": synth_proposal["example_sql"],
                    "parameters": synth_proposal.get("parameters", []) or [],
                    "usage_guidance": synth_proposal.get("usage_guidance", ""),
                    "rationale": (
                        f"Cluster-driven synthesis "
                        f"(archetype={synth_proposal.get('_archetype_name', '?')}). "
                        f"Root cause: {root_cause}. {coordination_notes}"
                    ),
                    "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                    "confidence": 0.85,
                    "questions_fixed": 1,
                    "questions_at_risk": 0,
                    "net_impact": 0.85,
                    "asi": {
                        "failure_type": "asset_routing_error",
                        "blame_set": source_clusters,
                        "severity": "major",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    },
                    "kit_id": synth_proposal.get("kit_id", ""),
                    "target_qids": synth_proposal.get("target_qids", []),
                    "rca_id": synth_proposal.get("rca_id", ""),
                    "provenance": {
                        **provenance_base,
                        "patch_type": "add_example_sql",
                        "synthesis_source": "cluster_driven",
                        "archetype": synth_proposal.get("_archetype_name", ""),
                        "source_cluster_id": synth_proposal.get(
                            "_cluster_id",
                            source_cluster.get("cluster_id", ""),
                        ),
                        "kit_id": synth_proposal.get("kit_id", ""),
                        "target_qids": synth_proposal.get("target_qids", []),
                        "rca_id": synth_proposal.get("rca_id", ""),
                    },
                })

                _append_teaching_kit_support_proposals(
                    proposals,
                    synth_proposal=synth_proposal,
                    provenance_base=provenance_base,
                    ag_id=ag_id,
                    source_cluster_id=synth_proposal.get(
                        "_cluster_id",
                        source_cluster.get("cluster_id", ""),
                    ),
                )

            if ENABLE_RCA_EXAMPLE_SQL_SYNTHESIS:
                try:
                    # Plan 11: flag-OFF path uses the legacy archetype
                    # catalog; flag-ON path routes to the LLM-first
                    # diagnose/cluster/synthesize stages via the
                    # _dispatch_plan11_synthesis_for_legacy_cluster
                    # adapter. The plan11_llm_first_enabled() guard
                    # keeps the import lazy on the legacy path and
                    # idempotent under flag-OFF replay.
                    from genie_space_optimizer.common.config import (
                        plan11_llm_first_enabled,
                    )
                    if plan11_llm_first_enabled():
                        _dispatch_synth = (
                            _dispatch_plan11_synthesis_for_legacy_cluster
                        )
                    else:
                        from genie_space_optimizer.optimization.cluster_driven_synthesis import (
                            run_cluster_driven_synthesis_for_single_cluster,
                        )
                        _dispatch_synth = (
                            run_cluster_driven_synthesis_for_single_cluster
                        )

                    for _theme in _rca_themes_requesting_synthesis(
                        metadata_snapshot.get("_rca_themes") or [],
                        target_qids=_rca_bridge_target_qids,
                    ):
                        _cluster = _cluster_from_rca_example_theme(_theme)
                        # P3 task 1: read .proposal from the typed
                        # ClusterSynthesisResult to preserve legacy
                        # dict-or-None semantics at this call site.
                        _synth_result_rca = _dispatch_synth(
                            _cluster,
                            metadata_snapshot,
                            benchmarks=benchmarks,
                            catalog=catalog,
                            gold_schema=gold_schema,
                            warehouse_id=warehouse_id,
                            w=w,
                            spark=spark,
                        )
                        _proposal = _synth_result_rca.proposal
                        if not _proposal:
                            continue
                        _proposal["source"] = "rca_teaching_kit"
                        _proposal.setdefault(
                            "rca_id", str(getattr(_theme, "rca_id", "")),
                        )
                        _proposal.setdefault(
                            "target_qids",
                            list(getattr(_theme, "target_qids", ()) or ()),
                        )
                        _proposal.setdefault(
                            "provenance",
                            {
                                **provenance_base,
                                "patch_type": "add_example_sql",
                                "synthesis_source": "rca_teaching_kit",
                                "rca_id": _proposal.get("rca_id", ""),
                                "kit_id": _proposal.get("kit_id", ""),
                            },
                        )
                        _proposal.setdefault(
                            "proposal_id",
                            f"P{len(proposals) + 1:03d}",
                        )
                        _proposal.setdefault("cluster_id", ag_id)
                        _proposal.setdefault("lever", 5)
                        _proposal.setdefault("scope", "genie_config")
                        _proposal.setdefault(
                            "change_description",
                            f"[{ag_id}] RCA synthesized example SQL",
                        )
                        _proposal.setdefault(
                            "dual_persistence",
                            DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                        )
                        proposals.append(_proposal)

                        _append_teaching_kit_support_proposals(
                            proposals,
                            synth_proposal=_proposal,
                            provenance_base=provenance_base,
                            ag_id=ag_id,
                            source_cluster_id=_proposal.get("_cluster_id", ""),
                        )
                except Exception:
                    logger.debug("RCA example SQL synthesis failed", exc_info=True)

        # ── Lever 6: SQL Expressions ─────────────────────────────────────
        elif target_lever == 6:
            ag_directives = action_group.get("lever_directives", {}).get("6", {})
            strategist_hints = ag_directives.get("sql_expressions", []) if isinstance(ag_directives, dict) else []

            # Plan 12 — extracted, survival-contract-threaded L6 generator.
            # Yields dicts already stamped with rca_card_id, causal_target,
            # repair_hypothesis, original_patch_type, and target_qids
            # from the upstream cluster context. Per-proposal shape fields
            # (proposal_id / scope / change_description / provenance) are
            # applied here for parity with the Stage-3 callsite at
            # optimizer.py:17776.
            proposals_l6 = _generate_proposals_for_lever6(
                action_group=action_group,
                metadata_snapshot=metadata_snapshot,
                ag_id=ag_id,
                spark=spark,
                catalog=catalog,
                gold_schema=gold_schema,
                warehouse_id=warehouse_id,
                w=w,
                benchmarks=benchmarks,
            )
            for proposal in proposals_l6:
                proposal["proposal_id"] = f"P{len(proposals) + 1:03d}"
                proposal["scope"] = "genie_config"
                proposal["change_description"] = (
                    f"[{ag_id}] RCA failed-row SQL Expression: "
                    f"{proposal.get('display_name', 'unnamed')} "
                    f"({proposal.get('snippet_type', '?')})"
                )
                proposal["proposed_value"] = proposal.get("sql", "")
                proposal["dual_persistence"] = DUAL_PERSIST_PATHS.get(
                    6,
                    DUAL_PERSIST_PATHS[5],
                )
                proposal["questions_at_risk"] = 0
                proposal["net_impact"] = max(
                    proposal.get("questions_fixed", 0) * 0.7,
                    1.0,
                )
                proposal["provenance"] = {
                    **provenance_base,
                    "patch_type": proposal["patch_type"],
                    "synthesis_source": "rca_failed_question_sql",
                    "source_question_id": proposal.get("source_question_id", ""),
                    # Plan 12 — provenance now records the threaded RCA
                    # fields so postmortems can prove the L6 lane saw them.
                    "rca_card_id": proposal.get("rca_card_id", ""),
                    "causal_target": proposal.get("causal_target", ""),
                }
                proposals.append(proposal)

            source_cids = set(action_group.get("source_cluster_ids", []))
            all_clusters = (
                metadata_snapshot.get("_failure_clusters")
                or metadata_snapshot.get("failure_clusters")
                or []
            )
            eligible_clusters = [
                c for c in all_clusters
                if c.get("cluster_id") in source_cids
            ]
            if not eligible_clusters:
                eligible_clusters = [
                    {"root_cause": root_cause, "question_traces": affected_qs, "cluster_id": ag_id}
                ]

            for cluster in eligible_clusters:
                # Plan 9 Task 4 — derive per-cluster typed LLM cluster
                # from the harness-stamped map so the L6 short-circuit
                # at _generate_lever6_proposal (optimizer.py:14122) can fire.
                _l6_cluster_id = str(cluster.get("cluster_id") or "")
                _l6_llm_cluster = None
                if llm_cluster_by_cluster_id and _l6_cluster_id:
                    _l6_llm_cluster = llm_cluster_by_cluster_id.get(
                        _l6_cluster_id
                    )
                proposal = _generate_lever6_proposal(
                    cluster, metadata_snapshot,
                    strategist_hints=strategist_hints,
                    w=w, spark=spark, catalog=catalog,
                    gold_schema=gold_schema, warehouse_id=warehouse_id,
                    benchmarks=benchmarks,
                    # Plan 9 Task 4 — Plan-5 typed inputs.
                    rca_evidence_typed=rca_evidence_typed,
                    llm_cluster=_l6_llm_cluster,
                    ag_id=ag_id,
                    iteration=iteration,
                    run_id=run_id,
                )
                if proposal:
                    proposal["proposal_id"] = f"P{len(proposals) + 1:03d}"
                    proposal["cluster_id"] = cluster.get("cluster_id", ag_id)
                    proposal["scope"] = "genie_config"
                    proposal["change_description"] = (
                        f"[{ag_id}] SQL Expression: {proposal.get('display_name', 'unnamed')} "
                        f"({proposal.get('snippet_type', '?')})"
                    )
                    proposal["proposed_value"] = proposal.get("sql", "")
                    proposal["rationale"] = proposal.get("rationale", f"Strategy: {root_cause}")
                    proposal["dual_persistence"] = DUAL_PERSIST_PATHS.get(6, DUAL_PERSIST_PATHS[5])
                    proposal["questions_at_risk"] = 0
                    proposal["net_impact"] = max(proposal.get("questions_fixed", 0) * 0.7, 1.0)
                    proposal["asi"] = {
                        "failure_type": cluster.get("root_cause", root_cause),
                        "blame_set": source_clusters,
                        "severity": "major",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    }
                    proposal["provenance"] = {**provenance_base, "patch_type": proposal["patch_type"]}
                    proposals.append(proposal)

            if ENABLE_RCA_SQL_SNIPPET_BRIDGE:
                _bridged_themes = _rca_themes_requesting_sql_snippets(
                    metadata_snapshot.get("_rca_themes") or [],
                    target_qids=_rca_bridge_target_qids,
                )
                for _theme in _bridged_themes:
                    _theme_patches = list(getattr(_theme, "patches", ()) or ())
                    _snippet_patches = [
                        p for p in _theme_patches
                        if isinstance(p, dict)
                        and p.get("type") in _RCA_SQL_SNIPPET_PATCH_TYPES
                    ]
                    if not _snippet_patches:
                        continue
                    _theme_kind = getattr(_theme, "rca_kind", None)
                    _kind_value = (
                        _theme_kind.value
                        if hasattr(_theme_kind, "value")
                        else str(_theme_kind or "unknown")
                    )
                    _theme_qids = list(getattr(_theme, "target_qids", ()) or ())
                    _theme_touched = list(
                        getattr(_theme, "touched_objects", ()) or ()
                    )
                    _synthetic_cluster = {
                        "cluster_id": str(getattr(_theme, "rca_id", ag_id)),
                        "root_cause": _kind_value,
                        "asi_failure_type": _kind_value,
                        "question_traces": _theme_qids,
                        "question_ids": _theme_qids,
                        "asi_blame_set": _theme_touched,
                    }
                    _hints: list[dict] = []
                    for _p in _snippet_patches:
                        _target_obj = str(_p.get("target_object") or "")
                        _target_table = ""
                        if "." in _target_obj:
                            _parts = _target_obj.split(".")
                            if len(_parts) >= 2:
                                _target_table = _parts[-2]
                        _hints.append({
                            "snippet_type": str(_p.get("snippet_type") or ""),
                            "target_table": _target_table,
                            "target_object": _target_obj,
                            "intent": _p.get("intent") or "",
                            "expected_objects": _p.get("expected_objects") or [],
                            "rca_kind": _kind_value,
                            "affected_questions": _theme_qids,
                        })
                    try:
                        # Plan 9 Task 4 — derive per-cluster typed LLM
                        # cluster so the L6 short-circuit fires.
                        _l6_syn_cluster_id = str(
                            _synthetic_cluster.get("cluster_id") or ""
                        )
                        _l6_syn_llm_cluster = None
                        if llm_cluster_by_cluster_id and _l6_syn_cluster_id:
                            _l6_syn_llm_cluster = llm_cluster_by_cluster_id.get(
                                _l6_syn_cluster_id
                            )
                        _proposal = _generate_lever6_proposal(
                            _synthetic_cluster, metadata_snapshot,
                            strategist_hints=_hints,
                            w=w, spark=spark, catalog=catalog,
                            gold_schema=gold_schema, warehouse_id=warehouse_id,
                            benchmarks=benchmarks,
                            # Plan 9 Task 4 — Plan-5 typed inputs.
                            rca_evidence_typed=rca_evidence_typed,
                            llm_cluster=_l6_syn_llm_cluster,
                            ag_id=ag_id,
                            iteration=iteration,
                        )
                    except Exception:
                        logger.debug(
                            "RCA SQL snippet bridge failed for theme %s",
                            getattr(_theme, "rca_id", "?"), exc_info=True,
                        )
                        _proposal = None
                    if not _proposal:
                        continue
                    _proposal["proposal_id"] = f"P{len(proposals) + 1:03d}"
                    _proposal["cluster_id"] = _synthetic_cluster["cluster_id"]
                    _proposal["scope"] = "genie_config"
                    _proposal["change_description"] = (
                        f"[{ag_id}] RCA SQL Expression: "
                        f"{_proposal.get('display_name', 'unnamed')} "
                        f"({_proposal.get('snippet_type', '?')})"
                    )
                    _proposal["proposed_value"] = _proposal.get("sql", "")
                    _proposal["rationale"] = (
                        _proposal.get("rationale")
                        or f"RCA-driven snippet for {_kind_value}"
                    )
                    _proposal["dual_persistence"] = DUAL_PERSIST_PATHS.get(
                        6, DUAL_PERSIST_PATHS[5],
                    )
                    _proposal["questions_at_risk"] = 0
                    _proposal["net_impact"] = max(
                        _proposal.get("questions_fixed", 0) * 0.7, 1.0,
                    )
                    _proposal["asi"] = {
                        "failure_type": _kind_value,
                        "blame_set": _theme_touched,
                        "severity": "major",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    }
                    _proposal["rca_id"] = str(getattr(_theme, "rca_id", ""))
                    _proposal["patch_family"] = str(
                        getattr(_theme, "patch_family", "")
                    )
                    _proposal["target_qids"] = _theme_qids
                    _proposal["source"] = "rca_theme_lever6"
                    _proposal["provenance"] = {
                        **provenance_base,
                        "patch_type": _proposal["patch_type"],
                        "synthesis_source": "rca_theme_lever6",
                        "rca_id": _proposal["rca_id"],
                        "patch_family": _proposal["patch_family"],
                    }
                    proposals.append(_proposal)

        # ── Example SQL from any lever ────────────────────────────────────
        # Preserve the originating lever so patches are attributed correctly
        # (e.g. TVF routing example SQLs stay under lever 3, not lever 5).
        if target_lever != 5 and isinstance(lever_dir, dict):
            ex_sqls = lever_dir.get("example_sqls", [])
            if not ex_sqls:
                legacy_ex = lever_dir.get("example_sql")
                if isinstance(legacy_ex, dict):
                    ex_sqls = [legacy_ex]
            if not isinstance(ex_sqls, list):
                ex_sqls = [ex_sqls] if isinstance(ex_sqls, dict) else []
            for ex_idx, ex_sql in enumerate(ex_sqls):
                if not isinstance(ex_sql, dict):
                    continue
                eq = (ex_sql.get("question") or "").strip()
                es = (ex_sql.get("sql_sketch") or "").strip()
                if eq and es:
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": f"{ag_id}_L{target_lever}_EX{ex_idx + 1}",
                        "lever": target_lever,
                        "scope": "genie_config",
                        "patch_type": "add_example_sql",
                        "change_description": f"[{ag_id}] Lever {target_lever} example SQL {ex_idx + 1}: {eq[:80]}",
                        "proposed_value": eq,
                        "example_question": eq,
                        "example_sql": es,
                        "parameters": ex_sql.get("parameters", []),
                        "usage_guidance": ex_sql.get("usage_guidance", ""),
                        "rationale": f"Example SQL from lever {target_lever}: {root_cause}",
                        "dual_persistence": DUAL_PERSIST_PATHS.get(target_lever, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.75,
                        "questions_fixed": 1,
                        "questions_at_risk": 0,
                        "net_impact": 0.75,
                        "asi": {
                            "failure_type": "asset_routing_error",
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "add_example_sql"},
                    })

        proposals = _validate_lever5_proposals(
            proposals, metadata_snapshot,
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id,
            benchmarks=benchmarks,
        )
        proposals = _deduplicate_proposals(proposals)
        proposals = _filter_no_op_proposals(proposals, metadata_snapshot)
        proposals.sort(key=lambda p: p.get("net_impact", 0), reverse=True)

        # Cycle 8 Bug 1 Phase 2 — stamp ``target_qids`` on every proposal
        # before it leaves this function. Standard L1-L4 paths build
        # proposal dicts without ``target_qids``; ``_backfill_patch_causal_metadata``
        # later defaults them to ``ag.affected_questions`` on the patch side
        # (harness.py:6663-6668), but anywhere downstream that reads
        # ``proposal.target_qids`` between proposal-emit and patch-backfill
        # (a 600-line gap, including the replay-fixture snapshot) used to
        # see ``[]``. The RCA-bridge / cluster-driven / RCA-forced L5 paths
        # already stamp explicit narrower ``target_qids`` (often via
        # ``_theme_qids``); the defaulting below preserves those narrower
        # values and only fills in for the standard-lever proposals.
        _ag_default_target_qids = [str(q) for q in (affected_qs or []) if q]
        if _ag_default_target_qids:
            for _proposal in proposals:
                _existing = _proposal.get("target_qids") or _proposal.get(
                    "_grounding_target_qids"
                ) or []
                if not [q for q in _existing if q]:
                    _proposal["target_qids"] = list(_ag_default_target_qids)

        span.set_outputs({
            "proposal_count": len(proposals),
            "proposal_types": [p.get("patch_type", "?") for p in proposals],
            "tables_affected": sorted({p.get("table", "") for p in proposals if p.get("table")}),
        })
        logger.info(
            "[%s] Lever %d generated %d proposal(s) from strategy directives",
            ag_id, target_lever, len(proposals),
        )

    return _prune_doa_fingerprints(
        proposals,
        buffer=doa_fingerprint_buffer,
        ag_id=str(ag_id),
    )


def generate_metadata_proposals(
    clusters: list[dict],
    metadata_snapshot: dict,
    target_lever: int | None = None,
    apply_mode: str = APPLY_MODE,
    w: WorkspaceClient | None = None,
    failed_levers: set[int] | None = None,
    lever_changes: list[dict] | None = None,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    benchmarks: list[dict] | None = None,
    warehouse_id: str = "",
) -> list[dict]:
    """Generate metadata change proposals from failure clusters.

    For each cluster, maps to a lever, calls the LLM to generate a concrete
    ``proposed_value``, resolves scope, and scores by net_impact.

    When *target_lever* is 5, uses **holistic mode**: a single LLM call
    synthesizes ALL evaluation learnings into a coherent instruction document
    (``rewrite_instruction``) plus targeted example SQL proposals. The
    *lever_changes* parameter provides context about what levers 1-4 already
    fixed in this iteration.

    When *failed_levers* is provided and *target_lever* is 5, clusters whose
    natural lever is in *failed_levers* are also included so that lever 5
    (instructions / example SQL) can act as a catch-all.
    """
    failed_levers = failed_levers or set()
    _pre_dedup = len(clusters)
    clusters = _deduplicate_clusters(clusters)
    if len(clusters) < _pre_dedup:
        logger.info(
            "Cluster dedup: %d -> %d (merged %d duplicates)",
            _pre_dedup, len(clusters), _pre_dedup - len(clusters),
        )
    proposals: list[dict] = []
    if target_lever == 5:
        desc_proposals = _detect_instruction_content_in_description(metadata_snapshot)
        proposals.extend(desc_proposals)

    # ── Holistic path for lever 5 ─────────────────────────────────────
    if target_lever == 5:
        all_lever5_clusters: list[dict] = []
        for cluster in clusters:
            natural_lever = _map_to_lever(
                cluster["root_cause"],
                asi_failure_type=cluster.get("asi_failure_type"),
                blame_set=cluster.get("asi_blame_set"),
                judge=cluster.get("affected_judge"),
            )
            if natural_lever == 5 or natural_lever in failed_levers:
                all_lever5_clusters.append(cluster)

        # Plan 9 Task 4 — NOTE: this _select_lever_5_holistic_path call
        # site lives inside generate_metadata_proposals (the ASI-driven
        # path used by propose_patch_set_from_asi), NOT inside
        # generate_proposals_from_strategy (the harness's main path).
        # The Plan-5 typed-input wire-in for the harness path goes
        # through the L6 short-circuit call sites inside
        # generate_proposals_from_strategy (see the L6 branches in
        # the elif target_lever == 6 block — those already receive
        # rca_evidence_typed / llm_cluster / ag_id / iteration). This
        # call site stays kwarg-free until a future plan threads
        # Plan-5 typed inputs through propose_patch_set_from_asi.
        holistic_result = _select_lever_5_holistic_path(
            all_clusters=all_lever5_clusters if all_lever5_clusters else clusters,
            metadata_snapshot=metadata_snapshot,
            lever_changes=lever_changes,
            w=w,
            benchmarks=benchmarks,
        )

        instruction_text = holistic_result.get("instruction_text", "")
        if instruction_text:
            instruction_text = _sanitize_plaintext_instructions(instruction_text)
        example_proposals = holistic_result.get("example_sql_proposals", [])
        rationale = holistic_result.get("rationale", "")

        from genie_space_optimizer.optimization.applier import _get_general_instructions

        current_instructions = _get_general_instructions(metadata_snapshot)

        if instruction_text:
            if (
                current_instructions
                and len(current_instructions.strip()) > 50
                and len(instruction_text.strip()) < len(current_instructions.strip()) * _INSTR_LOSS_THRESHOLD
            ):
                logger.warning(
                    "Holistic instruction rewrite would lose content (%d -> %d chars) "
                    "— force-merging existing instructions",
                    len(current_instructions), len(instruction_text),
                )
                instruction_text = _merge_structured_instructions(
                    existing=current_instructions,
                    contributions=[instruction_text],
                )

            if current_instructions and not _instruction_coverage(
                current_instructions, instruction_text
            ):
                logger.warning(
                    "Holistic instruction rewrite drops key phrases — force-merging"
                )
                instruction_text = _merge_structured_instructions(
                    existing=current_instructions,
                    contributions=[instruction_text],
                )

            total_q = sum(len(c.get("question_ids", [])) for c in all_lever5_clusters)
            holistic_traces = []
            for c in (all_lever5_clusters or clusters):
                holistic_traces.extend(c.get("question_traces", []))
            proposals.append({
                "proposal_id": f"P{len(proposals) + 1:03d}",
                "cluster_id": "HOLISTIC_L5",
                "lever": 5,
                "scope": "genie_config",
                "patch_type": "rewrite_instruction",
                "change_description": f"Holistic instruction rewrite ({len(instruction_text)} chars)",
                "proposed_value": instruction_text,
                "old_value": current_instructions,
                "rationale": rationale,
                "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                "confidence": 0.8,
                "questions_fixed": total_q,
                "questions_at_risk": 0,
                "net_impact": max(total_q * 0.8, 1.0),
                "asi": {
                    "failure_type": "missing_instruction",
                    "blame_set": [],
                    "severity": "major",
                    "counterfactual_fixes": [],
                    "ambiguity_detected": False,
                },
                "provenance": {
                    "cluster_id": "HOLISTIC_L5",
                    "root_cause": "missing_instruction",
                    "originating_questions": holistic_traces,
                    "lever": 5,
                    "lever_name": "Genie Space Instructions",
                    "patch_type": "rewrite_instruction",
                },
            })

        for idx, ex in enumerate(example_proposals):
            if not isinstance(ex, dict):
                continue
            eq = (ex.get("example_question") or "").strip()
            es = (ex.get("example_sql") or "").strip()
            if not eq or not es:
                continue
            proposals.append({
                "proposal_id": f"P{len(proposals) + 1:03d}",
                "cluster_id": f"HOLISTIC_EX_{idx + 1:03d}",
                "lever": 5,
                "scope": "genie_config",
                "patch_type": "add_example_sql",
                "change_description": f"Example SQL: {eq[:80]}",
                "proposed_value": eq,
                "example_question": eq,
                "example_sql": es,
                "parameters": ex.get("parameters", []),
                "usage_guidance": ex.get("usage_guidance", ""),
                "rationale": rationale,
                "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                "confidence": 0.75,
                "questions_fixed": 1,
                "questions_at_risk": 0,
                "net_impact": 0.75,
                "asi": {
                    "failure_type": "asset_routing_error",
                    "blame_set": [],
                    "severity": "major",
                    "counterfactual_fixes": [],
                    "ambiguity_detected": False,
                },
            })

        # Bug #4 — verbatim benchmark mining removed from Lever 5 proposal
        # generation. Example SQLs come exclusively from AFS-gated structural
        # synthesis (Phase 3), not from copying benchmark expected_sql.

        proposals = _validate_lever5_proposals(
            proposals, metadata_snapshot,
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id,
            benchmarks=benchmarks,
        )
        _pre_dedup_p = len(proposals)
        proposals = _deduplicate_proposals(proposals)
        if len(proposals) < _pre_dedup_p:
            logger.info(
                "Proposal dedup: %d -> %d (removed %d duplicates)",
                _pre_dedup_p, len(proposals), _pre_dedup_p - len(proposals),
            )
        proposals.sort(key=lambda p: p["net_impact"], reverse=True)
        return proposals

    # ── Standard per-cluster path (levers 1-4) ───────────────────────
    # Phase 1.3: ``_MAX_CLUSTERS_PER_LEVER`` was a hard cap of 3, which
    # silently dropped a 4th cluster even when each cluster had a
    # distinct root_cause and deserved its own proposal (observed in
    # iter-1 log: H001 ``wrong_measure`` was dropped because H002/H003/H004
    # also mapped to Lever 1).  Lift the floor to the number of distinct
    # root_causes among eligible clusters so every distinct failure mode
    # gets at least one proposal slot.
    _MIN_CLUSTER_BUDGET = 3
    eligible_clusters: list[tuple[dict, int]] = []
    for cluster in clusters:
        natural_lever = _map_to_lever(
            cluster["root_cause"],
            asi_failure_type=cluster.get("asi_failure_type"),
            blame_set=cluster.get("asi_blame_set"),
            judge=cluster.get("affected_judge"),
        )
        lever = natural_lever
        if target_lever is not None and lever != target_lever:
            continue
        eligible_clusters.append((cluster, lever))

    eligible_clusters.sort(key=lambda x: len(x[0]["question_ids"]), reverse=True)
    _distinct_root_causes = {
        c.get("root_cause", "other") for c, _ in eligible_clusters
    }
    _max_clusters = max(_MIN_CLUSTER_BUDGET, len(_distinct_root_causes))
    if len(eligible_clusters) > _max_clusters:
        logger.info(
            "Capping clusters for lever %s: %d -> %d "
            "(distinct_root_causes=%d, floor=%d)",
            target_lever, len(eligible_clusters), _max_clusters,
            len(_distinct_root_causes), _MIN_CLUSTER_BUDGET,
        )
        # Preserve at least one cluster per distinct root_cause before
        # filling remaining slots by question count.
        _kept: list[tuple[dict, int]] = []
        _seen_rc: set[str] = set()
        for c, lv in eligible_clusters:
            _rc = c.get("root_cause", "other")
            if _rc not in _seen_rc:
                _kept.append((c, lv))
                _seen_rc.add(_rc)
            if len(_kept) >= _max_clusters:
                break
        # Fill any remaining slots with the highest-question-count clusters.
        if len(_kept) < _max_clusters:
            for c, lv in eligible_clusters:
                if (c, lv) in _kept:
                    continue
                _kept.append((c, lv))
                if len(_kept) >= _max_clusters:
                    break
        eligible_clusters = _kept

    for cluster, lever in eligible_clusters:

        failure_type = cluster.get("asi_failure_type") or cluster.get("root_cause", "other")
        patch_type = _LEVER_TO_PATCH_TYPE.get(
            (failure_type, lever),
            _LEVER_TO_PATCH_TYPE.get(
                (failure_type, 1), "add_instruction"
            ),
        )

        llm_result = _call_llm_for_proposal(cluster, metadata_snapshot, patch_type, lever, w=w)

        extra_fields: dict = {}

        if lever == 4 and isinstance(llm_result.get("join_spec"), dict):
            llm_result["join_spec"] = ensure_join_spec_fields(llm_result["join_spec"], config=metadata_snapshot)
            valid, reason = validate_join_spec_types(
                llm_result["join_spec"], metadata_snapshot
            )
            if not valid:
                logger.info(
                    "Reactive join proposal rejected (type mismatch): %s", reason
                )
                continue
            extra_fields["join_spec"] = llm_result["join_spec"]

        scope = _resolve_scope(lever, apply_mode)
        q_fixed = len(cluster["question_ids"])
        confidence = cluster["confidence"]
        total_objects = max(len(metadata_snapshot.get("tables", [])), 1)
        blast_radius = 1
        net_impact = q_fixed * confidence - 0.1 * (blast_radius / total_objects)
        rationale = llm_result.get("rationale", "")

        asi_block = {
            "failure_type": failure_type,
            "blame_set": cluster.get("asi_blame_set") or [],
            "severity": "major",
            "counterfactual_fixes": cluster.get("asi_counterfactual_fixes", []),
            "ambiguity_detected": cluster.get("root_cause") == "repeatability_issue",
        }

        if lever in (1, 2) and isinstance(llm_result.get("changes"), list):
            for change in llm_result["changes"]:
                if not isinstance(change, dict):
                    continue
                tbl = change.get("table", "")
                col = change.get("column", "")
                if not tbl or not col:
                    continue

                sections = change.get("sections")
                entity_type = change.get("entity_type", "")

                if isinstance(sections, dict) and sections:
                    section_keys = list(sections.keys())
                    change_desc = f"Update {tbl}.{col} sections={section_keys}"
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": cluster["cluster_id"],
                        "lever": lever,
                        "scope": scope,
                        "patch_type": patch_type,
                        "change_description": change_desc,
                        "proposed_value": "",
                        "rationale": rationale,
                        "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[5]),
                        "confidence": confidence,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": net_impact,
                        "asi": asi_block,
                        "provenance": _build_provenance(cluster, lever, patch_type),
                        "table": tbl,
                        "column": col,
                        "column_sections": sections,
                        "column_entity_type": entity_type,
                        **extra_fields,
                    })
                else:
                    desc = change.get("description")
                    syns = change.get("synonyms")
                    change_desc = f"Update {tbl}.{col}"
                    if desc:
                        change_desc += f" description={desc}"
                    if syns:
                        change_desc += f" synonyms={syns}"
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": cluster["cluster_id"],
                        "lever": lever,
                        "scope": scope,
                        "patch_type": patch_type,
                        "change_description": change_desc,
                        "proposed_value": desc[0] if isinstance(desc, list) and desc else "",
                        "rationale": rationale,
                        "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[5]),
                        "confidence": confidence,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": net_impact,
                        "asi": asi_block,
                        "provenance": _build_provenance(cluster, lever, patch_type),
                        "table": tbl,
                        "column": col,
                        "column_description": desc,
                        "column_synonyms": syns,
                        **extra_fields,
                    })

            for tbl_change in llm_result.get("table_changes") or []:
                if not isinstance(tbl_change, dict):
                    continue
                tbl = tbl_change.get("table", "")
                if not tbl:
                    continue
                tbl_sections = tbl_change.get("sections")
                if not isinstance(tbl_sections, dict) or not tbl_sections:
                    continue
                tbl_etype = tbl_change.get("entity_type", "table")
                change_desc = f"Update table {tbl} sections={list(tbl_sections.keys())}"
                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": cluster["cluster_id"],
                    "lever": lever,
                    "scope": scope,
                    "patch_type": "update_description",
                    "change_description": change_desc,
                    "proposed_value": "",
                    "rationale": rationale,
                    "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[5]),
                    "confidence": confidence,
                    "questions_fixed": q_fixed,
                    "questions_at_risk": 0,
                    "net_impact": net_impact,
                    "asi": asi_block,
                    "provenance": _build_provenance(cluster, lever, "update_description"),
                    "table": tbl,
                    "table_sections": tbl_sections,
                    "table_entity_type": tbl_etype,
                    **extra_fields,
                })
        else:
            proposed_value = (
                llm_result.get("proposed_value")
                or llm_result.get("instruction_text")
                or llm_result.get("example_question")
                or ""
            )

            proposal = {
                "proposal_id": f"P{len(proposals) + 1:03d}",
                "cluster_id": cluster["cluster_id"],
                "lever": lever,
                "scope": scope,
                "patch_type": patch_type,
                "change_description": proposed_value or _describe_fix(cluster),
                "proposed_value": proposed_value,
                "rationale": rationale,
                "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[5]),
                "confidence": confidence,
                "questions_fixed": q_fixed,
                "questions_at_risk": 0,
                "net_impact": net_impact,
                "asi": asi_block,
                "provenance": _build_provenance(cluster, lever, patch_type),
                **extra_fields,
            }
            proposals.append(proposal)

    if target_lever == 4:
        soft_clusters_for_joins = [
            c for c in clusters
            if c.get("source") == "soft_signal" or c.get("is_soft_signal")
        ]
        discovery_hints = discover_join_candidates(
            metadata_snapshot,
            soft_signal_clusters=soft_clusters_for_joins or None,
        )
        if discovery_hints:
            discovery_specs = _call_llm_for_join_discovery(
                metadata_snapshot, discovery_hints, w=w,
            )
            for spec_result in discovery_specs:
                join_spec = spec_result.get("join_spec")
                if not isinstance(join_spec, dict):
                    continue
                join_spec = ensure_join_spec_fields(join_spec, config=metadata_snapshot)
                spec_result["join_spec"] = join_spec

                valid, reason = validate_join_spec_types(join_spec, metadata_snapshot)
                if not valid:
                    logger.info("Discovery join rejected (type mismatch): %s", reason)
                    continue

                left_obj = join_spec.get("left", {})
                right_obj = join_spec.get("right", {})
                left_id = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
                right_id = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""
                sql_parts = join_spec.get("sql", [])
                condition = sql_parts[0] if sql_parts else ""

                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": f"JOIN_DISC_{len(proposals) + 1:03d}",
                    "lever": 4,
                    "scope": "genie_config",
                    "patch_type": "add_join_spec",
                    "change_description": f"Add join: {condition}" if condition else f"Add join: {left_id} ↔ {right_id}",
                    "proposed_value": "",
                    "rationale": spec_result.get("rationale", "LLM-assisted join discovery"),
                    "join_spec": join_spec,
                    "dual_persistence": DUAL_PERSIST_PATHS.get(4, DUAL_PERSIST_PATHS[5]),
                    "confidence": 0.7,
                    "questions_fixed": 0,
                    "questions_at_risk": 0,
                    "net_impact": 0.35,
                    "asi": {
                        "failure_type": "missing_join_spec",
                        "blame_set": [],
                        "severity": "minor",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    },
                })

    proposals = _validate_lever5_proposals(
        proposals, metadata_snapshot,
        spark=spark, catalog=catalog, gold_schema=gold_schema,
        w=w, warehouse_id=warehouse_id,
        benchmarks=benchmarks,
    )
    _pre_dedup_p = len(proposals)
    proposals = _deduplicate_proposals(proposals)
    if len(proposals) < _pre_dedup_p:
        logger.info(
            "Proposal dedup: %d -> %d (removed %d duplicates)",
            _pre_dedup_p, len(proposals), _pre_dedup_p - len(proposals),
        )
    proposals = _merge_overlapping_instructions(proposals)
    proposals = _filter_no_op_proposals(proposals, metadata_snapshot)
    proposals.sort(key=lambda p: p["net_impact"], reverse=True)

    # Phase 4.2: post-LLM rationale uniqueness validation. The
    # ``_per_target_rationale`` helper above guarantees per-target
    # context within this function, but downstream paths
    # (``_call_llm_for_proposal``, holistic rewrite) can still produce
    # near-duplicate rationales when the LLM stamps the same strategy
    # summary on every proposal. When uniqueness drops below 50%, log
    # a structured warning and rewrite each rationale by appending the
    # patch-specific ``change_description`` so operators have at least
    # a target-aware breadcrumb on every proposal.
    if proposals:
        _rationales = [str(p.get("rationale") or "").strip() for p in proposals]
        _unique = len(set(_rationales))
        if _unique < max(1, len(proposals) // 2):
            logger.warning(
                "Phase 4.2: low rationale uniqueness (%d unique / %d proposals) "
                "— augmenting with target-specific change_description suffix",
                _unique, len(proposals),
            )
            for _p in proposals:
                _r = str(_p.get("rationale") or "").strip()
                _cd = str(_p.get("change_description") or "").strip()
                if _cd and _cd not in _r:
                    _p["rationale"] = (
                        f"{_r} | per-target: {_cd}" if _r else _cd
                    )

    return proposals


def propose_patch_set_from_asi(
    asi_rows: list[dict],
    metadata_snapshot: dict,
    lever: int | None = None,
) -> list[dict]:
    """Generate proposals directly from ASI records.

    Wraps ASI rows into cluster-like structures and delegates to
    ``generate_metadata_proposals``.
    """
    blame_groups: dict[tuple, list] = defaultdict(list)
    for row in asi_rows:
        if not isinstance(row, dict):
            continue
        ft = row.get("failure_type", row.get("asi_failure_type", "other"))
        bs = row.get("blame_set", [])
        if isinstance(bs, list):
            key = (ft, tuple(sorted(bs)))
        else:
            key = (ft, (str(bs),))
        blame_groups[key].append(row)

    clusters: list[dict] = []
    for (ft, bs_tuple), rows in blame_groups.items():
        if len(rows) < 1:
            continue
        clusters.append(
            {
                "cluster_id": f"ASI_C{len(clusters) + 1:03d}",
                "root_cause": ft,
                "question_ids": [
                    r.get("question_id", f"q{i}") for i, r in enumerate(rows)
                ],
                "affected_judge": rows[0].get("judge", "unknown"),
                "confidence": sum(
                    float(r.get("confidence", 0.5)) for r in rows
                )
                / max(len(rows), 1),
                "asi_failure_type": ft,
                "asi_blame_set": list(bs_tuple) if bs_tuple else None,
                "asi_counterfactual_fixes": [
                    r.get("counterfactual_fix", "")
                    for r in rows
                    if r.get("counterfactual_fix")
                ],
            }
        )

    return generate_metadata_proposals(
        clusters, metadata_snapshot, target_lever=lever
    )


# ═══════════════════════════════════════════════════════════════════════
# 5. Scoring (pure)
# ═══════════════════════════════════════════════════════════════════════


def score_patch_set(proposals: list[dict], metadata_snapshot: dict) -> float:
    """Score a patch set by expected impact.

    ``questions_blamed * avg_confidence - 0.1 * (blast_objects / total_objects)``
    """
    if not proposals:
        return 0.0

    total_objects = max(
        len(metadata_snapshot.get("tables", [])) if metadata_snapshot else 1, 1
    )

    all_targets: set[str] = set()
    questions_total = 0
    confidences: list[float] = []

    for p in proposals:
        target = p.get("target_object") or p.get("object_id") or p.get("target", "")
        if target:
            all_targets.add(target)
        questions_total += p.get("questions_fixed", 0)
        confidences.append(float(p.get("confidence", 0.5)))

    avg_confidence = sum(confidences) / max(len(confidences), 1)
    blast = len(all_targets)
    return questions_total * avg_confidence - 0.1 * (blast / total_objects)


# ═══════════════════════════════════════════════════════════════════════
# 6. Conflict Detection & Batching (pure)
# ═══════════════════════════════════════════════════════════════════════


def detect_conflicts_and_batch(proposals: list[dict]) -> list[list[dict]]:
    """Group proposals into conflict-free batches.

    Within each lever group, checks ``CONFLICT_RULES``.  Starts a new batch
    when a conflict is found.
    """
    conflict_set = set()
    for a, b in CONFLICT_RULES:
        conflict_set.add((a, b))
        conflict_set.add((b, a))

    batches: list[list[dict]] = []
    current_batch: list[dict] = []
    current_types: set[str] = set()

    for p in proposals:
        p_type = p.get("patch_type") or p.get("type", "")
        has_conflict = any((p_type, existing) in conflict_set for existing in current_types)
        if has_conflict:
            batches.append(current_batch)
            current_batch = [p]
            current_types = {p_type}
        else:
            current_batch.append(p)
            current_types.add(p_type)

    if current_batch:
        batches.append(current_batch)
    return batches


# ═══════════════════════════════════════════════════════════════════════
# 7. Regression Detection (pure)
# ═══════════════════════════════════════════════════════════════════════


def detect_regressions(
    current_scores: dict[str, float],
    previous_scores: dict[str, float],
    threshold: float = REGRESSION_THRESHOLD,
    skip_judges: set[str] | None = None,
) -> list[dict]:
    """Detect if any metric dropped more than ``threshold`` percentage points.

    Parameters
    ----------
    skip_judges : set[str] | None
        Judge names to exclude from regression checking.  Use this for
        informational judges whose convergence threshold is 0.0 (e.g.
        ``response_quality``) — they should not block progress.
    """
    regressions: list[dict] = []
    for key in previous_scores:
        if skip_judges and key in skip_judges:
            continue
        prev_val = previous_scores.get(key, 0)
        curr_val = current_scores.get(key, 0)
        if curr_val < prev_val - threshold:
            regressions.append(
                {
                    "judge": key,
                    "previous": prev_val,
                    "current": curr_val,
                    "drop": prev_val - curr_val,
                }
            )
    return regressions


# ═══════════════════════════════════════════════════════════════════════
# 8. Validation (pure)
# ═══════════════════════════════════════════════════════════════════════


def validate_patch_set(
    patches: list[dict], metadata_snapshot: dict | None = None
) -> tuple[bool, list[str]]:
    """Validate a patch set before application.

    Checks: known types, conflict rules, blast radius (max 5 objects),
    and optionally validates targets exist in metadata_snapshot.
    """
    errors: list[str] = []
    valid_types = set(PATCH_TYPES.keys())

    for i, p in enumerate(patches):
        pt = p.get("type") if isinstance(p, dict) else None
        if pt and pt not in valid_types:
            errors.append(f"Patch {i}: unknown type '{pt}'")

    targets: set[str] = set()
    for p in patches:
        if not isinstance(p, dict):
            continue
        obj = p.get("target_object") or p.get("object_id") or p.get("target") or p.get("table")
        if obj:
            targets.add(obj)

    if len(targets) > MAX_PATCH_OBJECTS:
        errors.append(
            f"Too many target objects: {len(targets)} (max {MAX_PATCH_OBJECTS})"
        )

    types_in_set = {p.get("type") for p in patches if isinstance(p, dict)}
    for a, b in CONFLICT_RULES:
        if a in types_in_set and b in types_in_set:
            errors.append(f"Conflicting patch types: {a} and {b}")

    if metadata_snapshot:
        known_tables = {
            t.get("name") or t.get("identifier", "")
            for t in metadata_snapshot.get("tables", [])
        }
        known_columns: set[str] = set()
        for t in metadata_snapshot.get("tables", []):
            for c in t.get("columns", t.get("column_configs", [])):
                known_columns.add(c.get("name") or c.get("column_name", ""))

        for i, p in enumerate(patches):
            if not isinstance(p, dict):
                continue
            tgt_table = p.get("table", "")
            tgt_col = p.get("column", "")
            if tgt_table and known_tables and tgt_table not in known_tables:
                errors.append(f"Patch {i}: table '{tgt_table}' not found in metadata")
            if tgt_col and known_columns and tgt_col not in known_columns:
                errors.append(f"Patch {i}: column '{tgt_col}' not found in metadata")

    return (len(errors) == 0, errors)


# ---------------------------------------------------------------------------
# Phase 2 Action 2.4 — Strategist coverage recall (focused second call).
# ---------------------------------------------------------------------------


def build_strategy_recall_addendum(
    *,
    uncovered_cluster_ids: tuple[str, ...],
) -> str:
    """Phase 2 Action 2.4 — build the strategist-recall addendum text
    that prepends the focused second call's prompt.

    Raises ``ValueError`` when called with zero uncovered clusters
    (caller bug — there is nothing to recall for).
    """
    if not uncovered_cluster_ids:
        raise ValueError(
            "build_strategy_recall_addendum: uncovered_cluster_ids must be non-empty"
        )
    ids_str = ", ".join(str(c) for c in uncovered_cluster_ids)
    return (
        "STRATEGIST RECALL: You previously emitted no AG for the following "
        f"hard clusters: [{ids_str}]. Each cluster has structured RCA "
        "evidence and at least one ASI counterfactual fix. For EACH cluster "
        "in this recall, you MUST emit one of:\n"
        "  (a) an action_group containing at least one applyable patch with "
        "a target_qids set covering the cluster's qids, OR\n"
        "  (b) a brief explanation in the cluster's `recall_skip_reason` "
        "field stating why no patch is applyable.\n"
        "Do NOT silently omit a cluster — every uncovered cluster MUST be "
        "addressed."
    )


def call_llm_for_strategy_focused(
    clusters: list[dict],
    *,
    uncovered_cluster_ids: tuple[str, ...],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    w: "WorkspaceClient | None" = None,
) -> dict:
    """Phase 2 Action 2.4 — focused second strategist call.

    Delegates to ``_call_llm_for_strategy`` with two adjustments:
    1. ``clusters`` is filtered to only the uncovered subset.
    2. The strategist prompt is prepended with the recall addendum
       (via the metadata_snapshot's ``_strategist_recall_addendum`` key
       which the underlying strategist's prompt builder reads).

    Returns the strategy dict produced by the underlying strategist
    (same shape — has ``action_groups``, etc.).
    """
    uncovered_set = {str(c) for c in uncovered_cluster_ids if str(c)}
    focused_clusters = [
        dict(c)
        for c in clusters
        if str(c.get("cluster_id") or "") in uncovered_set
    ]
    if not focused_clusters:
        return {"action_groups": [], "rationale": "no eligible uncovered clusters"}

    addendum = build_strategy_recall_addendum(
        uncovered_cluster_ids=uncovered_cluster_ids,
    )
    enriched_metadata = dict(metadata_snapshot or {})
    enriched_metadata["_strategist_recall_addendum"] = addendum

    return _call_llm_for_strategy(
        focused_clusters,
        list(soft_signal_clusters or []),
        enriched_metadata,
        w,
    )
