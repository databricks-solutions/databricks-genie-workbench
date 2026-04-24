"""Cluster-driven example SQL synthesis (Bug #4 Phase 3 — reactive).

Reactive counterpart to :mod:`optimization.preflight_synthesis`. Same
engine (synthesis prompt + 5-gate validator + P2 Genie-agreement gate +
firewall), different trigger: invoked per-cluster when the lever loop's
Lever 5 strategist emits ``example_sqls`` for an action group. Replaces
the historical "verbatim-from-strategist" path at
:mod:`optimization.optimizer` (search for the Lever 5 example_sqls_list
loop in ``generate_proposals_from_strategy``).

Design invariants (enforced structurally + tested):

A. **Does NOT apply proposals directly.** Every function in this module
   returns proposal dicts for the Lever 5 pipeline to apply. The
   pre-flight applier ``_apply_preflight_proposals`` is intentionally
   never called here — synthesized proposals flow through the same
   ``_validate_lever5_proposals`` + ``_deduplicate_proposals`` +
   downstream patch applier as every other Lever 5 proposal.

B. **``space_id`` travels via ``metadata_snapshot["_space_id"]``.** The
   Lever 5 caller (:func:`generate_proposals_from_strategy`) does not
   accept a space_id parameter; threading one through would touch every
   strategist call site. Instead the harness stamps
   ``metadata_snapshot["_space_id"] = space_id`` at iteration start,
   and this module reads it defensively.

C. **Shared budget counter across action groups.**
   ``metadata_snapshot["_cluster_synthesis_count"]`` is incremented on
   every synthesis attempt (success or failure) and checked against
   :data:`common.config.CLUSTER_SYNTHESIS_PER_ITERATION`. The harness
   resets the counter to 0 at the top of each iteration.

D. **Missing-join-spec fallback.** If AFS.blame_set implies a join-
   bearing archetype but no matching ``instructions.join_specs`` entry
   exists, the planner retries archetype selection with the extra
   table removed (single-table fallback) before giving up. Prevents
   the archetype's structural gate from rejecting every synthesized SQL
   in the common ``missing_join`` / ``wrong_join`` cluster case.

Leak safety: AFS is leak-free by construction (stripped by
:func:`optimization.afs.format_afs` and re-asserted by
:func:`optimization.afs.validate_afs`). The cluster-driven prompt
prepends the AFS block to the byte-equivalent pre-flight prompt output —
no new placeholders, no benchmark text path.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

from genie_space_optimizer.common.config import (
    CLUSTER_SYNTHESIS_PER_ITERATION,
    EXAMPLE_QUESTION_SQLS_SAFETY_CAP,
    PREFLIGHT_COLUMN_COVERAGE_K,
)
from genie_space_optimizer.optimization.afs import format_afs, validate_afs
from genie_space_optimizer.optimization.archetypes import (
    Archetype,
    pick_archetype,
)
# Module-level imports on the engine surface so tests can
# ``patch("cluster_driven_synthesis.validate_synthesis_proposal")`` at
# the dispatcher's attribute. Imports the same symbols pre-flight uses
# so both triggers share exactly one engine.
from genie_space_optimizer.optimization.preflight_synthesis import (
    AssetSlice,
    _top_k_columns,
    render_preflight_prompt,
)
from genie_space_optimizer.optimization.synthesis import (
    GateResult,
    validate_synthesis_proposal,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# AFS block rendering (leak-safe — input is a format_afs output only)
# ═══════════════════════════════════════════════════════════════════════


def render_afs_block(afs: dict) -> str:
    """Render an AFS dict as a leak-safe block for the synthesis prompt.

    The input MUST be the output of :func:`format_afs`, which strips
    raw benchmark text by construction. ``validate_afs`` asserts this
    at runtime — we don't re-validate here to avoid the cost; callers
    that build an AFS from untrusted input must validate before passing.

    ``structural_diff`` is a nested dict and is rendered as indented
    JSON rather than ``repr()`` so the LLM sees a proper tree and not a
    Python dict-literal. Mirrors how
    :func:`optimization.synthesis.render_synthesis_prompt` handles the
    same field.
    """
    if not afs:
        return ""
    lines: list[str] = []
    cluster_id = str(afs.get("cluster_id") or "?")
    failure_type = str(afs.get("failure_type") or "unknown")
    affected_judge = str(afs.get("affected_judge") or "unknown")
    blame = afs.get("blame_set") or []
    blame_str = ", ".join(str(b) for b in blame if b) if blame else "(none)"
    suggested_fix = str(afs.get("suggested_fix_summary") or "").strip()
    counterfactuals = afs.get("counterfactual_fixes") or []

    lines.append(f"  Cluster ID: {cluster_id}")
    lines.append(f"  Failure type: {failure_type}")
    lines.append(f"  Affected judge: {affected_judge}")
    lines.append(f"  Blamed objects: {blame_str}")
    if suggested_fix:
        lines.append(f"  Suggested fix: {suggested_fix}")
    if counterfactuals:
        lines.append("  Counterfactual fixes:")
        for f in counterfactuals[:3]:
            lines.append(f"    - {str(f)[:200]}")
    # structural_diff is dict-shaped; render as indented JSON so dict
    # repr noise doesn't leak into the prompt.
    diff = afs.get("structural_diff") or {}
    if diff:
        try:
            diff_text = json.dumps(diff, indent=4, default=str)
        except Exception:
            diff_text = str(diff)
        lines.append(f"  Structural diff:\n{diff_text}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════
# ClusterContext — SynthesisContext for the cluster-driven trigger
# ═══════════════════════════════════════════════════════════════════════


@dataclass
class ClusterContext:
    """SynthesisContext wrapping (AFS + derived AssetSlice).

    Satisfies the :class:`SynthesisContext` protocol: exposes
    :meth:`to_identifier_allowlist` and :meth:`asset_ids` via the
    inner :class:`AssetSlice`. The AFS is not part of the protocol —
    cluster-driven rendering prepends it to the pre-flight prompt via
    :func:`render_cluster_driven_prompt`, so the pre-flight prompt
    template stays byte-equivalent whether or not AFS is present.

    Attributes
    ----------
    afs : dict
        Output of :func:`format_afs`. Leak-free by construction.
    asset_slice : AssetSlice
        Narrowed schema view derived from ``afs.blame_set`` + snapshot.
    cluster_id : str
        Copied from ``afs.cluster_id`` for convenience logging.
    """

    afs: dict
    asset_slice: AssetSlice
    cluster_id: str = "?"
    # Phase 2.R2c: warehouse-sampled ``_data_profile`` threaded through so
    # cluster-driven synthesis inherits the same value-grounded prompt
    # section the pre-flight path now enjoys. Optional so legacy call
    # sites without a profile continue to render ``(no profile available)``.
    data_profile: dict | None = None

    def to_identifier_allowlist(self) -> str:
        return self.asset_slice.to_identifier_allowlist()

    def asset_ids(self) -> list[str]:
        return self.asset_slice.asset_ids()


# ═══════════════════════════════════════════════════════════════════════
# Prompt wrapper — prepends AFS to the pre-flight prompt
# ═══════════════════════════════════════════════════════════════════════


def render_cluster_driven_prompt(
    archetype: Archetype,
    context: ClusterContext,
    existing_questions: list[str],
    *,
    retry_feedback: str | None = None,
) -> str:
    """Render the cluster-driven prompt: AFS block + pre-flight prompt.

    Internally passes ``context.asset_slice`` (an ``AssetSlice``) to the
    pre-flight renderer — the pre-flight template accesses concrete
    ``AssetSlice`` attributes (``.tables``, ``.metric_view`` …) via its
    private formatters, so we cannot substitute a ``ClusterContext``
    there without plumbing delegation through every formatter. Keeping
    the AFS concern at this wrapper keeps the pre-flight template and
    its formatter helpers completely untouched.

    ``retry_feedback`` passes through to the pre-flight renderer for the
    R6 retry path on cluster-driven synthesis.

    Byte-equivalence contract: when ``render_afs_block`` returns empty
    (no AFS fields beyond cluster_id), this function returns the pre-
    flight prompt verbatim, preserving the invariant that pre-flight's
    prompt is unchanged whether or not AFS is present. Tested in
    ``test_preflight_prompt_bytes_equivalent_without_afs``.
    """
    base = render_preflight_prompt(
        archetype, context.asset_slice, existing_questions,
        data_profile=context.data_profile,
        retry_feedback=retry_feedback,
    )
    afs_block = render_afs_block(context.afs)
    if not afs_block:
        return base
    return (
        "## Failure signature (AFS) — this example must address this failure\n"
        f"{afs_block}\n\n"
        + base
    )


# ═══════════════════════════════════════════════════════════════════════
# AssetSlice derivation from AFS.blame_set
# ═══════════════════════════════════════════════════════════════════════


def _resolve_asset_by_identifier(
    metadata_snapshot: dict, identifier: str,
) -> dict | None:
    """Find a table or metric view snapshot by FQ or short identifier."""
    if not identifier:
        return None
    ident = identifier.strip().lower()
    short = ident.split(".")[-1]
    ds = metadata_snapshot.get("data_sources", {}) or {}
    for bucket in ("tables", "metric_views"):
        for t in ds.get(bucket, []) or []:
            if not isinstance(t, dict):
                continue
            tid = (t.get("identifier") or t.get("name") or "").strip().lower()
            if tid == ident or tid.split(".")[-1] == short:
                return t
    return None


def _find_matching_join_spec(
    metadata_snapshot: dict, left_id: str, right_id: str,
) -> dict | None:
    """Return the first join_spec whose (left, right) covers both ids.

    Match is order-insensitive — a join spec with left=A,right=B matches
    a blame_set of {B, A}.
    """
    a = (left_id or "").strip().lower()
    b = (right_id or "").strip().lower()
    if not a or not b:
        return None
    want = {a, b}
    specs = (
        (metadata_snapshot.get("instructions") or {}).get("join_specs") or []
    )
    for js in specs:
        if not isinstance(js, dict):
            continue
        left = (js.get("left") or {}).get("identifier", "").strip().lower()
        right = (js.get("right") or {}).get("identifier", "").strip().lower()
        if {left, right} == want:
            return js
    return None


def _derive_asset_slice_from_afs(
    afs: dict,
    metadata_snapshot: dict,
    *,
    column_k: int = PREFLIGHT_COLUMN_COVERAGE_K,
) -> tuple[AssetSlice, Archetype] | None:
    """Build an :class:`AssetSlice` + archetype pair from an AFS.

    Returns ``None`` when no archetype matches the cluster (caller
    falls back to ``instruction_only_fallback`` or text instruction).

    Missing-join-spec fallback (Invariant D): when ``blame_set`` has
    two tables but no matching ``instructions.join_specs`` entry exists,
    the archetype's ``has_joinable`` requirement cannot be satisfied
    cleanly. We retry by removing the second table from consideration;
    if the reduced single-table scope still matches a non-JOIN
    archetype, we use that. Otherwise return ``None``.
    """
    archetype = pick_archetype(afs, metadata_snapshot)
    if archetype is None:
        return None

    blame = [str(b) for b in (afs.get("blame_set") or []) if b]
    resolved = [
        _resolve_asset_by_identifier(metadata_snapshot, b) for b in blame
    ]
    resolved = [a for a in resolved if a is not None]

    # Partition into tables and MVs.
    ds = metadata_snapshot.get("data_sources", {}) or {}
    mv_ids = {
        (mv.get("identifier") or "").strip().lower()
        for mv in (ds.get("metric_views") or []) if isinstance(mv, dict)
    }
    tables = [
        a for a in resolved
        if (a.get("identifier") or "").strip().lower() not in mv_ids
    ]
    mvs = [
        a for a in resolved
        if (a.get("identifier") or "").strip().lower() in mv_ids
    ]
    primary_mv = mvs[0] if mvs else None

    # Two-table case: check for matching join_spec.
    if len(tables) >= 2:
        left_id = (tables[0].get("identifier") or "").strip()
        right_id = (tables[1].get("identifier") or "").strip()
        js = _find_matching_join_spec(metadata_snapshot, left_id, right_id)
        if js is not None:
            columns = (
                _top_k_columns(tables[0], column_k)
                + _top_k_columns(tables[1], column_k)
            )
            return (
                AssetSlice(
                    tables=tables[:2],
                    metric_view=primary_mv,
                    columns=columns,
                    join_spec=js,
                ),
                archetype,
            )
        # No matching join_spec — fall through to the single-table
        # fallback below. The originally-picked archetype may have
        # required ``has_joinable``; re-pick with a reduced blame_set
        # so we don't force a JOIN archetype against missing join info.
        logger.info(
            "cluster-driven: no join_spec for blame_set=%s — falling back to "
            "single-table synthesis",
            [left_id, right_id],
        )
        # Build a single-table AFS view for re-picking the archetype.
        reduced_afs = dict(afs)
        reduced_afs["blame_set"] = [blame[0]] if blame else []
        archetype_single = pick_archetype(reduced_afs, metadata_snapshot)
        if archetype_single is None:
            return None
        columns = _top_k_columns(tables[0], column_k)
        return (
            AssetSlice(
                tables=tables[:1],
                metric_view=primary_mv,
                columns=columns,
            ),
            archetype_single,
        )

    # One-table (or MV-only) case.
    if tables:
        columns = _top_k_columns(tables[0], column_k)
        return (
            AssetSlice(
                tables=tables[:1],
                metric_view=primary_mv,
                columns=columns,
            ),
            archetype,
        )
    if primary_mv is not None:
        columns = _top_k_columns(primary_mv, column_k)
        return (
            AssetSlice(
                tables=[],
                metric_view=primary_mv,
                columns=columns,
            ),
            archetype,
        )

    # blame_set referenced only assets we couldn't resolve — no slice possible.
    logger.info(
        "cluster-driven: blame_set=%s did not resolve to any schema asset",
        blame,
    )
    return None


# ═══════════════════════════════════════════════════════════════════════
# Orchestrator — single cluster entry point (the one the Lever 5 intercept uses)
# ═══════════════════════════════════════════════════════════════════════


def _existing_example_count(metadata_snapshot: dict) -> int:
    instr = metadata_snapshot.get("instructions", {}) or {}
    examples = instr.get("example_question_sqls", []) or []
    return len([ex for ex in examples if isinstance(ex, dict)])


def _existing_questions(metadata_snapshot: dict) -> list[str]:
    """For the synthesis prompt's anti-dup hint list."""
    out: list[str] = []
    instr = metadata_snapshot.get("instructions", {}) or {}
    for ex in (instr.get("example_question_sqls", []) or []):
        if not isinstance(ex, dict):
            continue
        q = ex.get("question", "")
        if isinstance(q, list):
            q = " ".join(str(x) for x in q)
        text = str(q).strip()
        if text:
            out.append(text)
    return out


def _read_budget_count(metadata_snapshot: dict) -> int:
    return int(metadata_snapshot.get("_cluster_synthesis_count", 0) or 0)


def _bump_budget_count(metadata_snapshot: dict) -> None:
    metadata_snapshot["_cluster_synthesis_count"] = (
        _read_budget_count(metadata_snapshot) + 1
    )


def _log_summary(
    trigger: str,
    *,
    cluster_id: str,
    archetype: str,
    outcome: str,
    gate_results: list[GateResult] | None = None,
    applied: int = 0,
    skipped_reason: str = "",
    extra: dict | None = None,
) -> None:
    """Structured log line — shared schema with pre-flight.

    Format (parseable by grep/Loki):
    ``synthesis.summary trigger=cluster cluster_id=... archetype=...
     outcome=... passed_parse=... applied=N skipped_reason=...``.

    Pre-flight's existing ``preflight.synthesis.summary`` line is not
    moved in this PR — it stays as-is for log-parser compatibility. A
    follow-up can unify both under ``synthesis.summary trigger=...``
    once downstream consumers are migrated.
    """
    passed: dict[str, int] = {
        "parse": 0, "execute": 0, "structural": 0, "arbiter": 0,
        "firewall": 0, "genie_agreement": 0,
    }
    for gr in gate_results or []:
        if gr.passed and gr.gate in passed:
            passed[gr.gate] += 1
    fields = [
        f"trigger={trigger}",
        f"cluster_id={cluster_id}",
        f"archetype={archetype or '-'}",
        f"outcome={outcome}",
    ]
    for k, v in passed.items():
        fields.append(f"passed_{k}={v}")
    fields.append(f"applied={applied}")
    if skipped_reason:
        fields.append(f"skipped_reason={skipped_reason}")
    if extra:
        for k, v in extra.items():
            fields.append(f"{k}={v}")
    logger.info("synthesis.summary " + " ".join(fields))


def run_cluster_driven_synthesis_for_single_cluster(
    cluster: dict,
    metadata_snapshot: dict,
    *,
    benchmarks: list[dict] | None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    w: Any = None,
    spark: Any = None,
    llm_caller: Callable[[str], str] | None = None,
    genie_ask: Callable[[Any, str, str], dict] | None = None,
    warehouse_executor: Callable[[str], list[dict]] | None = None,
    arbiter: Callable[..., dict] | None = None,
) -> dict | None:
    """Synthesize ONE example-SQL proposal for ``cluster`` via the AFS engine.

    Pipeline — all invariants enforced here, not at the call site:

    1. **Safety cap** (Decision #4): if the space's current
       ``example_question_sqls`` count ≥ ``EXAMPLE_QUESTION_SQLS_SAFETY_CAP``,
       return None immediately. Caller falls back to text instruction.
    2. **Budget** (Invariant C): if ``metadata_snapshot["_cluster_synthesis_count"]``
       ≥ ``CLUSTER_SYNTHESIS_PER_ITERATION``, return None. Counter is
       NOT bumped for cap-hit skips (the iteration made no LLM call).
    3. **AFS projection** (Invariant) + runtime leak validation.
    4. **Archetype pick + slice derivation** (Invariant D: missing-
       join-spec fallback).
    5. **Synthesis** via :func:`synthesize_preflight_candidate` with
       cluster-driven prompt wrapper (AFS block prepended).
    6. **5-gate validation** via :func:`validate_synthesis_proposal`.
    7. **P2 arbiter gate** (Decision #2: always ON for cluster-driven)
       via :func:`_gate_genie_agreement`. ``space_id`` is read from
       ``metadata_snapshot["_space_id"]`` per Invariant B.

    Returns
    -------
    dict | None
        A proposal dict shaped for the Lever 5 pipeline (patch_type=
        ``"add_example_sql"``, example_question, example_sql,
        rationale, usage_guidance, provenance, and a sentinel
        ``_archetype_name`` for observability). ``None`` when any step
        declines — caller applies ``instruction_only_fallback``.

    Does NOT call ``_apply_preflight_proposals`` (Invariant A) — the
    Lever 5 pipeline runs ``_validate_lever5_proposals`` +
    ``_deduplicate_proposals`` + the shared applier on whatever this
    function returns.
    """
    cluster_id = str((cluster or {}).get("cluster_id") or "?")

    # ── Invariant safety checks ─────────────────────────────────────
    existing_count = _existing_example_count(metadata_snapshot)
    if existing_count >= EXAMPLE_QUESTION_SQLS_SAFETY_CAP:
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype="",
            outcome="skipped", skipped_reason=(
                f"safety_cap:{existing_count}>={EXAMPLE_QUESTION_SQLS_SAFETY_CAP}"
            ),
        )
        return None

    budget_used = _read_budget_count(metadata_snapshot)
    if budget_used >= CLUSTER_SYNTHESIS_PER_ITERATION:
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype="",
            outcome="skipped", skipped_reason=(
                f"budget:{budget_used}>={CLUSTER_SYNTHESIS_PER_ITERATION}"
            ),
        )
        return None

    # ── AFS projection + runtime leak validation ────────────────────
    try:
        afs = format_afs(cluster)
    except Exception:
        logger.warning(
            "cluster-driven: format_afs failed for cluster=%s",
            cluster_id, exc_info=True,
        )
        return None

    # Build benchmark corpus once — reused by validate_afs and the
    # firewall gate inside validate_synthesis_proposal.
    benchmark_corpus = None
    try:
        from genie_space_optimizer.optimization.leakage import BenchmarkCorpus
        benchmark_corpus = BenchmarkCorpus.from_benchmarks(benchmarks or [])
    except Exception:
        logger.warning(
            "cluster-driven: BenchmarkCorpus unavailable — firewall degrades to "
            "structural check only",
            exc_info=True,
        )

    # Leak assertion — raises on any string-field collision with benchmark corpus.
    try:
        validate_afs(afs, benchmark_corpus)
    except Exception as exc:
        logger.warning(
            "cluster-driven: validate_afs rejected cluster=%s — %s",
            cluster_id, exc,
        )
        return None

    # ── Archetype + slice derivation (Invariant D fallback inside) ─
    derived = _derive_asset_slice_from_afs(afs, metadata_snapshot)
    if derived is None:
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype="",
            outcome="skipped", skipped_reason="no_archetype_or_slice",
        )
        return None
    slice_, archetype = derived
    context = ClusterContext(
        afs=afs,
        asset_slice=slice_,
        cluster_id=cluster_id,
        data_profile=metadata_snapshot.get("_data_profile") or None,
    )

    # Bump budget counter — we're about to issue an LLM call.
    _bump_budget_count(metadata_snapshot)

    # ── Synthesize via the cluster-driven prompt wrapper ────────────
    # We build the final prompt here (AFS + pre-flight) then intercept
    # the ``llm_caller`` so ``synthesize_preflight_candidate`` sees the
    # wrapped prompt without us having to reimplement its LLM call /
    # JSON-extraction logic. This preserves the pre-flight renderer as-
    # is (byte-equivalent) while adding the AFS block exclusively for
    # the cluster-driven trigger.
    cluster_prompt = render_cluster_driven_prompt(
        archetype, context, _existing_questions(metadata_snapshot),
    )

    if llm_caller is None:
        # Production path: call through the traced LLM with the wrapped
        # prompt directly. Mirrors ``synthesize_preflight_candidate``'s
        # internal LLM call but with our AFS-prepended prompt.
        from genie_space_optimizer.optimization.optimizer import _traced_llm_call
        try:
            raw, _ = _traced_llm_call(
                w, "You are a SQL example author.", cluster_prompt,
                span_name="cluster_driven_example_synthesis",
            )
        except Exception:
            logger.warning(
                "cluster-driven: LLM call failed for cluster=%s archetype=%s",
                cluster_id, archetype.name, exc_info=True,
            )
            raw = ""
    else:
        raw = llm_caller(cluster_prompt)

    # Reuse synthesis.py's robust JSON extractor — same as pre-flight.
    from genie_space_optimizer.optimization.synthesis import _extract_json_proposal
    proposal = _extract_json_proposal(raw) if raw else None
    if proposal is not None:
        proposal.setdefault("patch_type", archetype.patch_type)
        if "usage_guidance" not in proposal:
            proposal["usage_guidance"] = str(proposal.get("rationale") or "").strip()

    if proposal is None:
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype=archetype.name,
            outcome="synth_none",
        )
        return None

    # ── 5-gate validation ──────────────────────────────────────────
    slice_allowlist = set(context.asset_slice.asset_ids())
    passed, gate_results = validate_synthesis_proposal(
        proposal,
        archetype=archetype,
        benchmark_corpus=benchmark_corpus,
        metadata_snapshot=metadata_snapshot,
        blame_set=afs.get("blame_set"),
        spark=spark, catalog=catalog, gold_schema=gold_schema,
        w=w, warehouse_id=warehouse_id,
        identifier_allowlist=slice_allowlist,
    )

    # ── Phase 3.R6: one retry on EMPTY_RESULT ──────────────────────
    # Mirrors the pre-flight retry in :mod:`preflight_synthesis`. We
    # rebuild the cluster-driven prompt with the retry-feedback block
    # (which passes through to the pre-flight renderer).
    if not passed:
        first_fail = next((g for g in gate_results if not g.passed), None)
        feedback: str | None = None
        if (
            first_fail is not None
            and first_fail.gate == "execute"
            and "EMPTY_RESULT" in (first_fail.reason or "")
        ):
            from genie_space_optimizer.optimization.preflight_synthesis import (
                _build_empty_result_feedback,
            )
            feedback = _build_empty_result_feedback(
                proposal, context.data_profile, context.asset_slice,
            ) or None
        else:
            # Phase 2.R6: also retry on unqualified / unresolved identifier
            # failures with the slice's identifier allowlist as feedback.
            from genie_space_optimizer.optimization.preflight_synthesis import (
                _build_qualification_feedback,
                _is_qualification_failure,
            )
            if _is_qualification_failure(first_fail):
                feedback = _build_qualification_feedback(
                    proposal, context.asset_slice,
                    first_fail.reason or "",
                ) or None

        if feedback is not None:
            retry_prompt = render_cluster_driven_prompt(
                archetype, context, _existing_questions(metadata_snapshot),
                retry_feedback=feedback,
            )
            if llm_caller is None:
                from genie_space_optimizer.optimization.optimizer import _traced_llm_call
                try:
                    retry_raw, _ = _traced_llm_call(
                        w, "You are a SQL example author.", retry_prompt,
                        span_name="cluster_driven_example_synthesis_retry",
                    )
                except Exception:
                    logger.warning(
                        "cluster-driven: retry LLM call failed for cluster=%s archetype=%s",
                        cluster_id, archetype.name, exc_info=True,
                    )
                    retry_raw = ""
            else:
                retry_raw = llm_caller(retry_prompt)
            retry_proposal = (
                _extract_json_proposal(retry_raw) if retry_raw else None
            )
            if retry_proposal is not None:
                retry_proposal.setdefault("patch_type", archetype.patch_type)
                if "usage_guidance" not in retry_proposal:
                    retry_proposal["usage_guidance"] = str(
                        retry_proposal.get("rationale") or "",
                    ).strip()
                proposal = retry_proposal
                passed, gate_results = validate_synthesis_proposal(
                    retry_proposal,
                    archetype=archetype,
                    benchmark_corpus=benchmark_corpus,
                    metadata_snapshot=metadata_snapshot,
                    blame_set=afs.get("blame_set"),
                    spark=spark, catalog=catalog, gold_schema=gold_schema,
                    w=w, warehouse_id=warehouse_id,
                    identifier_allowlist=slice_allowlist,
                )

    if not passed:
        first_fail = next((g for g in gate_results if not g.passed), None)
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype=archetype.name,
            outcome="gate_fail",
            gate_results=gate_results,
            skipped_reason=f"gate:{first_fail.gate if first_fail else '?'}:{first_fail.reason if first_fail else ''}",
        )
        return None

    # ── P2 Genie-vs-synthesized arbiter gate (always ON) ───────────
    # Reads space_id per Invariant B; fail-closed when missing so the
    # caller falls back to instruction-only rather than silently
    # applying an un-arbitered proposal.
    space_id = (
        metadata_snapshot.get("_space_id")
        or metadata_snapshot.get("space_id")
        or ""
    )
    if not space_id:
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype=archetype.name,
            outcome="gate_fail",
            gate_results=gate_results,
            skipped_reason="missing_space_id",
        )
        return None

    from genie_space_optimizer.optimization.preflight_synthesis import (
        _gate_genie_agreement,
    )
    agreement = _gate_genie_agreement(
        proposal,
        space_id=space_id,
        w=w, warehouse_id=warehouse_id,
        catalog=catalog, gold_schema=gold_schema,
        metadata_snapshot=metadata_snapshot,
        genie_ask=genie_ask,
        warehouse_executor=warehouse_executor,
        arbiter=arbiter,
    )
    if not agreement.passed:
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype=archetype.name,
            outcome="arbiter_reject",
            gate_results=list(gate_results) + [agreement],
            skipped_reason=f"genie_agreement:{agreement.reason}",
        )
        return None

    # ── Success — shape a Lever 5 proposal dict ────────────────────
    final = {
        "patch_type": "add_example_sql",
        "example_question": str(proposal.get("example_question", "")).strip(),
        "example_sql": str(proposal.get("example_sql", "")).strip(),
        "parameters": proposal.get("parameters", []) or [],
        "usage_guidance": str(proposal.get("usage_guidance") or proposal.get("rationale") or "").strip(),
        "rationale": str(proposal.get("rationale", "")).strip(),
        # Sentinel for the Lever 5 intercept + observability. Prefixed
        # so it's clear this is not a persisted field on the proposal.
        "_archetype_name": archetype.name,
        "_cluster_id": cluster_id,
    }
    _log_summary(
        "cluster", cluster_id=cluster_id, archetype=archetype.name,
        outcome="applied",
        gate_results=list(gate_results) + [agreement],
        applied=1,
    )
    return final
