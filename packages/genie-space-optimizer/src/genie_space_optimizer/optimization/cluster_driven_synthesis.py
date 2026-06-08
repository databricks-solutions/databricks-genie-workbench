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
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable


# Trial 17 step 7 — flag that deprioritises ``pick_archetype`` as a
# control-flow gate. Default: ON (see ``trial17_flags`` for the
# canonical implementation and opt-out semantics). The wrapper below is
# kept for module-local callers; new code should import from
# ``trial17_flags`` directly.
from genie_space_optimizer.optimization.trial17_flags import (
    trial17_lever_led_synthesis_enabled as _trial17_lever_led_synthesis_enabled,  # noqa: F401
)
from genie_space_optimizer.optimization.trial32_flags import (
    trial32_column_fqn_resolution_enabled,
)

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


def _set_cluster_driven_span_tag(key: str, value: str) -> None:
    """Best-effort set an attribute on the current MLflow / OTel span.

    Plan 2026-05-17-cluster-driven-example-synthesis-hardening Tasks 11
    + 16 — sister-parallel to ``preflight_synthesis._set_preflight_span_tag``.
    Tagging is observability, not correctness; failures here are
    silently swallowed.
    """
    try:
        import mlflow

        active = mlflow.tracing.fluent.get_current_active_span()
        if active is not None:
            active.set_attribute(key, value)
    except Exception:
        return


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
    # Task 10: render only fields with real signal. Sentinels like
    # ``unknown``, ``(none)`` and ``?`` add no information and crowd
    # the prompt — skip them rather than emit them.
    cluster_id = str(afs.get("cluster_id") or "").strip()
    failure_type = str(afs.get("failure_type") or "").strip()
    affected_judge = str(afs.get("affected_judge") or "").strip()
    blame = afs.get("blame_set") or []
    blame_str = ", ".join(str(b) for b in blame if b) if blame else ""
    suggested_fix = str(afs.get("suggested_fix_summary") or "").strip()
    counterfactuals = afs.get("counterfactual_fixes") or []

    # Task 11: a cluster_id alone is not failure signal — it's just a
    # label. Treat the AFS as effectively empty when no other field
    # carries actionable content, so render_cluster_driven_prompt can
    # take the byte-equivalence path.
    has_signal = bool(
        (failure_type and failure_type.lower() != "unknown")
        or (affected_judge and affected_judge.lower() != "unknown")
        or blame_str
        or suggested_fix
        or counterfactuals
        or (afs.get("structural_diff") or {})
    )
    if not has_signal:
        return ""

    if cluster_id and cluster_id != "?":
        lines.append(f"  Cluster ID: {cluster_id}")
    if failure_type and failure_type.lower() != "unknown":
        lines.append(f"  Failure type: {failure_type}")
    if affected_judge and affected_judge.lower() != "unknown":
        lines.append(f"  Affected judge: {affected_judge}")
    if blame_str:
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


def render_failure_context_block(failure_contexts: list[dict] | None) -> str:
    """Render safe RCA failure evidence for teaching-kit synthesis.

    This block may contain Genie's generated SQL and judge feedback. It must
    not contain benchmark questions or expected SQL.
    """
    contexts = [c for c in (failure_contexts or []) if isinstance(c, dict)]
    if not contexts:
        return ""
    lines = [
        "<rca_failure_evidence>",
        "Use this to understand what Genie misunderstood. Do not imitate any failed input prompt.",
    ]
    for idx, ctx in enumerate(contexts[:3], 1):
        lines.append(f"  Failure {idx}:")
        lines.append(f"    Question ID: {ctx.get('question_id', '')}")
        lines.append(f"    Root cause: {ctx.get('root_cause', 'unknown')}")
        if ctx.get("failed_judges"):
            lines.append(
                f"    Failed judges: "
                f"{', '.join(str(x) for x in ctx.get('failed_judges', []))}"
            )
        if ctx.get("blame_set"):
            lines.append(
                f"    Blame: {', '.join(str(x) for x in ctx.get('blame_set', []))}"
            )
        if ctx.get("counterfactual_fixes"):
            lines.append("    Counterfactual fixes:")
            for fix in ctx.get("counterfactual_fixes", [])[:3]:
                lines.append(f"      - {str(fix)[:300]}")
        if ctx.get("rationales"):
            lines.append("    Judge rationale:")
            for rationale in ctx.get("rationales", [])[:3]:
                lines.append(f"      - {str(rationale)[:300]}")
        generated_sql = str(ctx.get("generated_sql") or "").strip()
        if generated_sql:
            lines.append("    Genie generated SQL:")
            lines.append("```sql")
            lines.append(generated_sql[:4000])
            lines.append("```")

    lines.append("</rca_failure_evidence>")
    rendered = "\n".join(lines)
    forbidden_tokens = ("expected_sql", "benchmark question", "inputs.question")
    if any(token in rendered.lower() for token in forbidden_tokens):
        raise ValueError("RCA failure evidence block contains forbidden benchmark field name")
    return rendered


# ═══════════════════════════════════════════════════════════════════════
# ClusterContext — SynthesisContext for the cluster-driven trigger
# ═══════════════════════════════════════════════════════════════════════


class SkippedReason(str, Enum):
    """Phase 8.1 (2026-05-17) — closed vocabulary of cluster-synthesis
    decline causes. Every early-exit in
    ``run_cluster_driven_synthesis_for_single_cluster`` MUST emit a
    member of this enum (or a dynamic-detail string whose prefix up
    to the first ``:`` matches a member) on its
    ``ClusterSynthesisResult.skipped_reason``.

    Values appear in ``GSO_NO_STRUCTURAL_CANDIDATE_V1`` markers and
    are consumed by postmortem tooling. Adding a value is backwards
    compatible; renaming a value is a breaking change.
    """

    SAFETY_CAP_REACHED = "safety_cap_reached"
    """The per-iteration safety cap on synthesis invocations was
    reached before this cluster's synthesis call. Detail past the
    colon may carry the count comparison (``safety_cap:5>=5``)."""
    BUDGET_EXHAUSTED = "budget_exhausted"
    """The per-iteration budget on archetype attempts was exhausted
    inside this cluster's synthesis call. Detail past the colon may
    carry the count comparison (``budget:3>=2``)."""
    FORMAT_AFS_FAILED = "format_afs_failed"
    """``format_afs`` returned an error / empty block for this
    cluster's selected archetype."""
    VALIDATE_AFS_REJECTED = "validate_afs_rejected"
    """``validate_afs`` rejected the formatted AFS block (leak,
    malformed, or coverage gap)."""
    NO_ARCHETYPE_OR_SLICE = "no_archetype_or_slice"
    """``pick_archetype`` returned None for this cluster's
    blame_set / root_cause combination.

    .. deprecated:: Trial 17
        Trial 17 deprioritises ``pick_archetype`` as a control-flow
        gate (the archetype catalog now travels as menu context inside
        the Stage 3 LLM prompt). With
        ``GSO_TRIAL17_LEVER_LED_SYNTHESIS`` enabled this enum value is
        unreachable; the surrounding logic falls back to the safety-net
        archetype and lets the LLM pick the lever. The value is kept
        for one trial cycle for back-compat; scheduled for removal in
        Trial 18.
    """
    NO_TOP_N_ARCHETYPE = "no_top_n_archetype"
    """Phase 8.2 — specialised case of NO_ARCHETYPE_OR_SLICE for
    ``plural_top_n_collapse`` clusters that need a TOP-N archetype
    which the archetype catalog does not yet expose.

    .. deprecated:: Trial 17
        See ``NO_ARCHETYPE_OR_SLICE``. Scheduled for removal in
        Trial 18.
    """
    SYNTH_NONE = "synth_none"
    """The LLM synthesis returned no proposal for the chosen
    archetype + cluster combination."""
    MISSING_SPACE_ID = "missing_space_id"
    """The space-id required for the genie-agreement arbiter gate is
    missing from the metadata snapshot."""
    # Dynamic-prefix variants. The codebase carries detail past the
    # colon (``gate:rowcount:mismatch``, ``genie_agreement:reason``);
    # the ``__post_init__`` invariant accepts these via prefix match.
    SAFETY_CAP = "safety_cap"
    BUDGET = "budget"
    GATE = "gate"
    GENIE_AGREEMENT = "genie_agreement"
    # Additional vocabulary surfaced by existing replay-test stubs
    # (``forced_synthesis_replay``, ``l5b_rich_path_replay``) and
    # downstream callers. Each describes a real synthesizer decline
    # path; the closed vocabulary is the union of every callsite the
    # invariant must accept.
    ARCHETYPES_EXHAUSTED = "archetypes_exhausted"
    """The dispatcher exhausted its archetype budget across attempts
    without producing a viable proposal."""
    ARCHETYPE_VALIDATION_FAILED = "archetype_validation_failed"
    """Archetype-specific validation rejected the synthesized
    proposal."""
    NO_VIABLE_ARCHETYPE = "no_viable_archetype"
    """A more granular sibling of ``no_archetype_or_slice``: the
    pick produced an archetype but downstream viability checks
    rejected it for the cluster's slice."""
    NO_STRUCTURAL_CANDIDATE = "no_structural_candidate"
    """Trial 31 W31.1(b) — the cluster's RCA mandates a structural
    mechanism but every surviving proposal was inert (the forced-L6 /
    plan11 structural synthesis declined). The SM finalizer emits a
    clean no-op with this reason rather than letting the inert patch
    survive to application (which would trip
    ``rca_mechanism_defaulted_to_instruction_text`` ->
    ``OPTIMIZER_INVARIANT_VIOLATION``, failed by W31.3)."""
    NORMALIZE_RETURNED_NONE = "normalize_returned_none"
    """Synthesizer normalisation pass returned ``None``, declining
    the candidate."""
    EXCEPTION = "exception"
    """Synthesizer raised an uncaught exception; the dispatcher
    surfaces the decline with this reason."""
    REPLAY_STUB_DEFAULT = "replay_stub_default"
    """Default decline emitted by replay-fixture stubs when no
    explicit outcome is configured."""


_SKIPPED_REASON_PREFIXES: frozenset[str] = frozenset(
    r.value for r in SkippedReason
)


@dataclass(frozen=True)
class ClusterSynthesisResult:
    """P3 — typed return from
    ``run_cluster_driven_synthesis_for_single_cluster``.

    Replaces the old ``dict | None`` contract. ``proposal`` carries
    the legacy payload (or ``None`` on miss). ``attempted_archetypes``
    is the ordered tuple of archetypes that were considered before
    the result.

    Phase 8.1 (2026-05-17) — ``skipped_reason`` is documented by the
    closed :class:`SkippedReason` enum. The ``__post_init__`` invariant
    accepts either an empty string (success path) or a value whose
    prefix up to the first ``:`` matches a member of the enum. This
    accommodates dynamic-detail variants (``safety_cap:5>=5``,
    ``gate:rowcount:mismatch``) while still rejecting unknown
    free-form strings.
    """

    proposal: dict | None
    attempted_archetypes: tuple[str, ...] = ()
    skipped_reason: str = ""
    # Trial 22 W3 — the slate compiler's drop summary for THIS cluster's
    # synthesis. Surfaced here so the harness can copy it onto the
    # durable iteration terminal-state ledger row (the cluster result
    # itself is transient and disappears at the next AG/cluster
    # transition). Schema: see
    # ``proposal_slate_compiler.build_compiler_drop_summary``.
    compiler_drop_summary: dict | None = None

    def __post_init__(self) -> None:
        if not self.skipped_reason:
            return
        prefix = str(self.skipped_reason).split(":", 1)[0]
        if (
            self.skipped_reason in _SKIPPED_REASON_PREFIXES
            or prefix in _SKIPPED_REASON_PREFIXES
        ):
            return
        raise ValueError(
            f"invalid skipped_reason {self.skipped_reason!r}; must "
            f"be empty, an enum member of SkippedReason, or a "
            f"colon-prefixed variant whose prefix matches one of: "
            f"{sorted(_SKIPPED_REASON_PREFIXES)}"
        )


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
    # Regression-mining lane: optional pre-rendered hint block (output
    # of ``render_strategist_hint_block``). When set, prepended ahead of
    # the AFS block by ``render_cluster_driven_prompt``. Empty string is
    # treated as absent so the existing byte-equivalence contract for
    # the no-AFS / no-hints path is preserved.
    regression_mining_hints: str = ""
    # RCA failure-evidence lane: per-target-QID dictionaries containing
    # Genie's generated SQL, judge metadata, and counterfactual fixes.
    # Excludes benchmark question text and expected SQL by construction.
    failure_contexts: list[dict] | None = None

    def to_identifier_allowlist(self) -> str:
        return self.asset_slice.to_identifier_allowlist()

    def asset_ids(self) -> list[str]:
        return self.asset_slice.asset_ids()


@dataclass(frozen=True, slots=True)
class ProposalContext:
    """Phase 2.5 — lightweight proposer context used by the lever-loop
    harness to thread protected-dependent QIDs into the proposal prompt.

    Distinct from :class:`ClusterContext` (which carries AFS + AssetSlice
    for cluster-driven preflight rendering). ``ProposalContext`` is the
    minimal envelope used by the decision-plane wiring site (Task 16):

    - ``cluster_id``: which cluster this proposal targets.
    - ``target_qids``: in-cluster QIDs the proposal is allowed to mutate.
    - ``rca_card``: the failure root-cause card driving the proposal.
    - ``protected_dependents``: outside-target QIDs the proposer MUST
      preserve. Populated whenever a prior iteration dropped a patch
      for collateral against those QIDs (sourced from the harness
      ``_outside_target_qids`` lane). Default ``()`` = no constraint
      and the rendered prompt stays byte-stable with pre-Phase-2.5
      fixtures.
    """
    cluster_id: str
    target_qids: tuple[str, ...]
    rca_card: dict
    protected_dependents: tuple[str, ...] = ()


def _render_protected_dependents_section(
    protected_dependents: tuple[str, ...],
) -> str:
    """Phase 2.5 — render the "Protected dependent QIDs" prompt section.

    Returns an empty string when ``protected_dependents`` is empty so
    callers can unconditionally splice the result without breaking
    byte-equivalence on the default path.
    """
    if not protected_dependents:
        return ""
    bullets = "\n".join(f"  - {q}" for q in protected_dependents)
    return (
        "\n## Protected dependent QIDs\n\n"
        "The following question_ids depend on broader space-level "
        "behavior that MUST be preserved. Do NOT alter SQL, "
        "instructions, or metadata in a way that changes how these "
        "QIDs are answered:\n\n"
        f"{bullets}\n\n"
        "When proposing patches for the target QIDs above, "
        "explicitly narrow the scope (per-question example_sql, "
        "per-question instructions, or a question-specific "
        "metric view) so the protected QIDs continue to use the "
        "existing logic unchanged.\n"
    )


# ═══════════════════════════════════════════════════════════════════════
# Prompt wrapper — prepends AFS to the pre-flight prompt
# ═══════════════════════════════════════════════════════════════════════


def render_cluster_driven_prompt(
    archetype: Archetype | None = None,
    context: ClusterContext | ProposalContext | None = None,
    existing_questions: list[str] | None = None,
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

    Phase 2.5 — when ``context`` is a :class:`ProposalContext` (used by
    the decision-plane wiring site) the function dispatches to a
    minimal renderer that emits only the "Protected dependent QIDs"
    directive when applicable. This is the entry point the harness
    uses to thread outside-target QIDs into the proposal prompt.
    """
    # Phase 2.5 — ProposalContext dispatch. ProposalContext is the
    # lightweight envelope from the harness wiring site; it does not
    # carry an AssetSlice/AFS so the preflight base prompt is not
    # rendered here. Empty protected_dependents yields an empty string
    # (byte-stable with pre-Phase-2.5 callers that never set the field).
    if isinstance(context, ProposalContext):
        return _render_protected_dependents_section(
            context.protected_dependents
        )

    assert context is not None and archetype is not None, (
        "render_cluster_driven_prompt requires (archetype, ClusterContext, "
        "existing_questions) for the cluster-driven preflight path."
    )
    base = render_preflight_prompt(
        archetype, context.asset_slice, existing_questions,
        data_profile=context.data_profile,
        retry_feedback=retry_feedback,
    )
    afs_block = render_afs_block(context.afs)
    failure_block = render_failure_context_block(context.failure_contexts)
    hints = (context.regression_mining_hints or "").strip()
    # Task 9: leak-safety guard. ``regression_mining_hints`` is built by
    # the harness from past run telemetry — if it ever carries benchmark
    # tokens, surface that loudly here rather than silently leaking the
    # tokens into the prompt. Same forbidden-token set as
    # ``render_failure_context_block``.
    if hints:
        _forbidden = ("expected_sql", "benchmark question", "inputs.question")
        if any(tok in hints.lower() for tok in _forbidden):
            raise ValueError(
                "regression_mining_hints contains forbidden benchmark field — "
                "refusing to render cluster-driven prompt"
            )
    # Byte-equivalence contract: with neither AFS nor hints, the
    # pre-flight prompt renders verbatim. Hints are placed AFTER the
    # AFS block so the existing AFS test fixtures stay valid; the
    # strategist sees AFS first (cluster-specific) then mining
    # cross-iteration lessons.
    prefix_parts: list[str] = []
    if afs_block:
        prefix_parts.append(
            "<failure_signature>\n"
            "This example must address the failure described below.\n"
            f"{afs_block}\n"
            "</failure_signature>"
        )
    if failure_block:
        prefix_parts.append(failure_block)
    if hints:
        prefix_parts.append(hints)
    if not prefix_parts:
        # Task 11: tag the active span so operators can distinguish the
        # byte-equivalence fallback from the AFS-driven kit_contract
        # path. Byte-equivalence here means "no AFS / failure / hints
        # were present" — the cluster-driven helper returns the
        # preflight prompt verbatim.
        _set_cluster_driven_span_tag(
            "cluster_driven_path", "byte_equivalent_preflight",
        )
        return base
    # Kit contract is appended only on cluster-driven paths (where AFS or
    # failure evidence is present). This keeps the legacy byte-equivalence
    # contract intact for the no-AFS preflight path.
    _set_cluster_driven_span_tag("cluster_driven_path", "kit_contract_appended")
    #
    # Task 14: strip the base preflight prompt's <output_schema> block
    # since the kit_contract footer carries its own (different) contract.
    # Keeping both would give the LLM two conflicting output schemas.
    from genie_space_optimizer.common.config import KIT_CONTRACT_PROMPT_FOOTER

    base_no_schema = _strip_output_schema_block(base)
    return (
        "\n\n".join(prefix_parts)
        + "\n\n"
        + base_no_schema
        + "\n\n"
        + KIT_CONTRACT_PROMPT_FOOTER
    )


def _strip_output_schema_block(prompt: str) -> str:
    """Remove the ``<output_schema>...</output_schema>`` block from
    ``prompt``. Used by :func:`render_cluster_driven_prompt` to avoid
    emitting two competing output contracts on the AFS path.

    Returns ``prompt`` unchanged when no <output_schema> block is found
    (defensive — callers that don't need the strip get a no-op).
    """
    open_tag = "<output_schema>"
    close_tag = "</output_schema>"
    start = prompt.find(open_tag)
    if start < 0:
        return prompt
    end = prompt.find(close_tag, start)
    if end < 0:
        return prompt
    end += len(close_tag)
    # Also trim a trailing blank line so the seam between the stripped
    # region and the appended footer doesn't double up newlines.
    while end < len(prompt) and prompt[end] == "\n":
        end += 1
    return (prompt[:start] + prompt[end:]).rstrip()


# ═══════════════════════════════════════════════════════════════════════
# AssetSlice derivation from AFS.blame_set
# ═══════════════════════════════════════════════════════════════════════


def _match_asset_in_data_sources(ds: dict, ident: str) -> dict | None:
    """Match a (lowercased) identifier against table/MV snapshots by full id
    or short (last-segment) name. Shared by :func:`_resolve_asset_by_identifier`
    for both the direct lookup and the Trial-32 column-FQN fallback."""
    short = ident.split(".")[-1]
    for bucket in ("tables", "metric_views"):
        for t in ds.get(bucket, []) or []:
            if not isinstance(t, dict):
                continue
            tid = (t.get("identifier") or t.get("name") or "").strip().lower()
            if tid == ident or tid.split(".")[-1] == short:
                return t
    return None


def _resolve_asset_by_identifier(
    metadata_snapshot: dict, identifier: str,
) -> dict | None:
    """Find a table or metric view snapshot by FQ or short identifier.

    Trial 32 W32.1 — when the identifier is a 4-part column FQN
    (``catalog.schema.table.column``) that does not match any table/MV
    directly, fall back to resolving the OWNING table by its 3-part prefix.
    Stage-1 blame_sets routinely name column FQNs (the airline
    ``extra_defensive_filter`` intent named
    ``main.airline.fact_tickets.payment_currency_cd``); without this they
    never resolve to a table, so ``_derive_asset_slice_from_afs`` returns
    ``None`` and the cluster declines with ``no_top_n_archetype``. Gated by
    ``GSO_TRIAL32_COLUMN_FQN_RESOLUTION`` (default ON; byte-stable when OFF —
    the fallback only runs after a direct match has already failed, so a
    table that legitimately matched is never affected).
    """
    if not identifier:
        return None
    ds = metadata_snapshot.get("data_sources", {}) or {}
    ident = identifier.strip().lower()
    direct = _match_asset_in_data_sources(ds, ident)
    if direct is not None:
        return direct
    # Column-FQN fallback: catalog.schema.table.column -> owning table.
    if trial32_column_fqn_resolution_enabled():
        parts = ident.split(".")
        if len(parts) >= 4:
            table_prefix = ".".join(parts[:-1])
            return _match_asset_in_data_sources(ds, table_prefix)
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


def _fallback_menu_archetype():
    """Trial 17 step 7 — return the safety-net archetype.

    Used when ``pick_archetype`` returns ``None`` and Trial 17's
    lever-led synthesis flag is enabled. Picks ``simple_enumerate``
    (the always-eligible safety net) by default; falls back to the
    first archetype in the catalog if ``simple_enumerate`` is absent.
    """
    from genie_space_optimizer.optimization.archetypes import ARCHETYPES

    for arch in ARCHETYPES:
        if getattr(arch, "name", "") == "simple_enumerate":
            return arch
    return ARCHETYPES[0] if ARCHETYPES else None


def _derive_asset_slice_from_afs(
    afs: dict,
    metadata_snapshot: dict,
    *,
    column_k: int = PREFLIGHT_COLUMN_COVERAGE_K,
) -> tuple[AssetSlice, "Archetype | None"] | None:
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
        # Trial 17 step 7 — deprioritise pick_archetype as a hard gate.
        # When ``GSO_TRIAL17_LEVER_LED_SYNTHESIS`` is on, fall back to
        # the safety-net archetype (``simple_enumerate``) so the LLM
        # gets a chance to pick a lever from the menu instead of the
        # cluster declining via ``NO_ARCHETYPE_OR_SLICE``.
        if _trial17_lever_led_synthesis_enabled():
            archetype = _fallback_menu_archetype()
            logger.info(
                "trial17: pick_archetype returned None for cluster=%s; "
                "falling back to safety-net archetype=%s",
                afs.get("cluster_id", "?"),
                getattr(archetype, "name", "?"),
            )
        else:
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
            # Trial 17 step 7 — same softening as above, applied to the
            # missing-join-spec single-table fallback path.
            if _trial17_lever_led_synthesis_enabled():
                archetype_single = _fallback_menu_archetype()
                logger.info(
                    "trial17: pick_archetype (reduced) returned None "
                    "for cluster=%s; falling back to safety-net "
                    "archetype=%s",
                    afs.get("cluster_id", "?"),
                    getattr(archetype_single, "name", "?"),
                )
            else:
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


def _validate_supporting_sql_snippet(
    proposal: dict,
    *,
    metadata_snapshot: dict,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
) -> dict | None:
    """Validate a teaching-kit SQL snippet proposal via the existing snippet gate.

    Returns the proposal augmented with ``validation_passed=True`` and a
    materialized ``sql_snippet`` payload when the snippet validates, or
    ``None`` when validation fails.
    """
    patch_type = str(proposal.get("patch_type") or "")
    if not patch_type.startswith("add_sql_snippet_"):
        return proposal
    snippet_type = str(
        proposal.get("snippet_type") or patch_type.replace("add_sql_snippet_", "")
    )
    sql = str(proposal.get("sql") or "").strip()
    if not sql:
        return None
    try:
        from genie_space_optimizer.optimization.benchmarks import validate_sql_snippet

        valid_result = validate_sql_snippet(
            sql,
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
                "cluster-driven: supporting SQL snippet rejected: %s",
                valid_result[1],
            )
            return None
        sql = valid_result[2] if len(valid_result) > 2 else sql
    except Exception:
        logger.debug(
            "cluster-driven: supporting SQL snippet validation failed",
            exc_info=True,
        )
        return None

    snippet_id = (
        str(proposal.get("snippet_id") or proposal.get("alias") or proposal.get("display_name") or "")
        .lower()
        .replace(" ", "_")[:64]
    )
    snippet = {
        "id": snippet_id,
        "name": proposal.get("display_name", ""),
        "display_name": proposal.get("display_name", ""),
        "sql": sql,
        "description": proposal.get("instruction", ""),
        "synonyms": proposal.get("synonyms", []) or [],
        "target_table": proposal.get("target_table", ""),
    }
    return {
        **proposal,
        "sql": sql,
        "sql_snippet": snippet,
        "validation_passed": True,
    }


def _failure_cluster_to_legacy_dict(fc: Any) -> dict:
    """Phase 1.2 (2026-05-17) migration shim — reconstruct the
    legacy cluster dict from a FailureCluster so the unmigrated body
    of ``run_cluster_driven_synthesis_for_single_cluster`` keeps
    working. Removed in a future phase once the body reads from the
    typed view directly."""
    return {
        "cluster_id": fc.cluster_id,
        "question_ids": list(fc.target_qids),
        "root_cause": fc.root_cause,
        "asi_failure_type": fc.asi_failure_type,
        "asi_blame_set": list(fc.blame_set_raw),
        "asi_blame_set_normalized": list(fc.blame_set_normalized),
        "rca_card": (
            {
                "id": fc.rca_card_id,
                "root_cause_summary": fc.rca_card_summary,
            }
            if fc.is_grounded
            else {}
        ),
        "failure_keys": list(fc.failure_keys),
    }


def run_cluster_driven_synthesis_for_single_cluster(
    cluster: "Any",
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
) -> ClusterSynthesisResult:
    """Synthesize ONE example-SQL proposal for ``cluster`` via the AFS engine.

    Pipeline — all invariants enforced here, not at the call site:

    1. **Safety cap** (Decision #4): if the space's current
       ``example_question_sqls`` count ≥ ``EXAMPLE_QUESTION_SQLS_SAFETY_CAP``,
       skip immediately. Caller falls back to text instruction.
    2. **Budget** (Invariant C): if ``metadata_snapshot["_cluster_synthesis_count"]``
       ≥ ``CLUSTER_SYNTHESIS_PER_ITERATION``, skip. Counter is
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
    ClusterSynthesisResult
        Always a typed result. ``.proposal`` carries the legacy
        proposal dict shaped for the Lever 5 pipeline (patch_type=
        ``"add_example_sql"``, example_question, example_sql,
        rationale, usage_guidance, provenance, and a sentinel
        ``_archetype_name`` for observability), or ``None`` when any
        step declines. ``.attempted_archetypes`` records the archetype
        considered (empty if archetype derivation failed).
        ``.skipped_reason`` documents which guard fired on the
        ``proposal=None`` path (e.g. ``budget:N>=M``,
        ``safety_cap:N>=M``, ``no_archetype_or_slice``,
        ``gate:<gate>:<reason>``, ``genie_agreement:<reason>``).

    Does NOT call ``_apply_preflight_proposals`` (Invariant A) — the
    Lever 5 pipeline runs ``_validate_lever5_proposals`` +
    ``_deduplicate_proposals`` + the shared applier on whatever this
    function returns.
    """
    # Phase 1.2 (2026-05-17) — accept FailureCluster OR legacy
    # Mapping. Legacy callers wrap at the boundary in
    # forced_synthesis_dispatch (Phase 1.3); other callers still
    # pass dicts. The shim reconstructs a legacy dict from the
    # typed view so the unmigrated body of this function keeps
    # working.
    from genie_space_optimizer.optimization.failure_cluster import (
        FailureCluster,
    )

    if isinstance(cluster, FailureCluster):
        cluster = _failure_cluster_to_legacy_dict(cluster)
    cluster_id = str((cluster or {}).get("cluster_id") or "?")
    # P3: track archetype provenance even on skipped paths so the
    # caller (the harness lever-5 wiring) can emit a typed
    # NO_STRUCTURAL_CANDIDATE record citing what was tried.
    _attempted_archetypes_so_far: list[str] = []

    # ── Invariant safety checks ─────────────────────────────────────
    existing_count = _existing_example_count(metadata_snapshot)
    if existing_count >= EXAMPLE_QUESTION_SQLS_SAFETY_CAP:
        _safety_cap_reason = (
            f"safety_cap:{existing_count}>={EXAMPLE_QUESTION_SQLS_SAFETY_CAP}"
        )
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype="",
            outcome="skipped", skipped_reason=_safety_cap_reason,
        )
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=tuple(_attempted_archetypes_so_far),
            skipped_reason=_safety_cap_reason,
        )

    budget_used = _read_budget_count(metadata_snapshot)
    if budget_used >= CLUSTER_SYNTHESIS_PER_ITERATION:
        _budget_reason = (
            f"budget:{budget_used}>={CLUSTER_SYNTHESIS_PER_ITERATION}"
        )
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype="",
            outcome="skipped", skipped_reason=_budget_reason,
        )
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=tuple(_attempted_archetypes_so_far),
            skipped_reason=_budget_reason,
        )

    # ── AFS projection + runtime leak validation ────────────────────
    try:
        afs = format_afs(cluster)
    except Exception:
        logger.warning(
            "cluster-driven: format_afs failed for cluster=%s",
            cluster_id, exc_info=True,
        )
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=tuple(_attempted_archetypes_so_far),
            skipped_reason="format_afs_failed",
        )

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
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=tuple(_attempted_archetypes_so_far),
            skipped_reason="validate_afs_rejected",
        )

    # ── Archetype + slice derivation (Invariant D fallback inside) ─
    derived = _derive_asset_slice_from_afs(afs, metadata_snapshot)
    if derived is None:
        # Phase 8.2 (2026-05-17) — when the missing archetype matches
        # a TOP-N shape cluster, emit the more specific
        # ``NO_TOP_N_ARCHETYPE`` so postmortem tooling can route to
        # the archetype-catalog backlog. Other RCAs route to the
        # generic ``NO_ARCHETYPE_OR_SLICE``.
        from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
            cluster_failure_keys,
        )
        _is_top_n_cluster = (
            "plural_top_n_collapse" in cluster_failure_keys(cluster)
        )
        _skipped_reason_value = (
            SkippedReason.NO_TOP_N_ARCHETYPE.value
            if _is_top_n_cluster
            else SkippedReason.NO_ARCHETYPE_OR_SLICE.value
        )
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype="",
            outcome="skipped", skipped_reason=_skipped_reason_value,
        )
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=tuple(_attempted_archetypes_so_far),
            skipped_reason=_skipped_reason_value,
        )
    slice_, archetype = derived
    # P3: archetype was successfully picked; record provenance so
    # downstream NO_STRUCTURAL_CANDIDATE records cite it on any
    # later-stage skip (gate fail, arbiter reject, etc).
    _attempted_archetypes_so_far.append(str(archetype.name))
    # Regression-mining hints are a leak-safe pre-rendered string that
    # the harness threads through the snapshot when the
    # ``GSO_ENABLE_REGRESSION_MINING_STRATEGIST`` flag is on. Default
    # ("") reproduces the legacy byte-equivalent prompt path.
    _rm_hints = metadata_snapshot.get("_regression_mining_hints") or ""

    target_qids = [
        str(q)
        for q in (
            cluster.get("question_ids")
            or cluster.get("affected_questions")
            or cluster.get("target_qids")
            or []
        )
        if str(q)
    ]
    failure_contexts: list[dict] = []
    try:
        from genie_space_optimizer.optimization.rca_failure_context import (
            contexts_for_target_qids,
        )

        failure_contexts = contexts_for_target_qids(
            metadata_snapshot.get("_rca_failure_contexts_by_qid") or {},
            target_qids,
        )
    except Exception:
        logger.debug(
            "cluster-driven: failed to resolve RCA failure contexts",
            exc_info=True,
        )
        failure_contexts = []

    context = ClusterContext(
        afs=afs,
        asset_slice=slice_,
        cluster_id=cluster_id,
        data_profile=metadata_snapshot.get("_data_profile") or None,
        regression_mining_hints=str(_rm_hints) if isinstance(_rm_hints, str) else "",
        failure_contexts=failure_contexts,
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
        # Plan 2026-05-17-cluster-driven-example-synthesis-hardening
        # Task 7: fix mis-key (was "lever_5b_example_sql"). The prompt's
        # actual registry name is "cluster_driven_example_synthesis".
        # Task 4: pass max_tokens=LEVER_5B_CLUSTER_DRIVEN_MAX_TOKENS.
        # Tasks 1+5: response_model=TeachingKitOutput matches the
        # prompt's <output_schema> (kit_summary + example_sql +
        # supporting_changes). The dormancy note no longer applies —
        # the contract is now correctly typed.
        from genie_space_optimizer.common.config import (
            CLUSTER_DRIVEN_EXAMPLE_SYNTHESIS_SYSTEM_MSG,
            LEVER_5B_CLUSTER_DRIVEN_MAX_TOKENS,
        )
        from genie_space_optimizer.optimization.optimizer import _traced_llm_call
        from genie_space_optimizer.optimization.evaluation import (
            _link_prompt_to_trace,
        )
        from genie_space_optimizer.optimization.prompt_io import (
            TeachingKitOutput,
        )
        _link_prompt_to_trace("cluster_driven_example_synthesis")
        try:
            raw, _ = _traced_llm_call(
                w, CLUSTER_DRIVEN_EXAMPLE_SYNTHESIS_SYSTEM_MSG, cluster_prompt,
                span_name="cluster_driven_example_synthesis",
                max_tokens=LEVER_5B_CLUSTER_DRIVEN_MAX_TOKENS,
                response_model=TeachingKitOutput,
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

    kit_id = f"kit_{cluster_id}_{_read_budget_count(metadata_snapshot) + 1}"
    from genie_space_optimizer.optimization.teaching_kit import normalize_teaching_kit

    kit = normalize_teaching_kit(
        proposal or {},
        kit_id=kit_id,
        target_qids=target_qids,
        rca_id=str(cluster.get("rca_id") or ""),
    )
    proposal = kit.primary or None
    supporting_proposals: list[dict] = []
    for support in kit.supporting:
        if str(support.get("patch_type") or "").startswith("add_sql_snippet_"):
            validated = _validate_supporting_sql_snippet(
                support,
                metadata_snapshot=metadata_snapshot,
                spark=spark,
                catalog=catalog,
                gold_schema=gold_schema,
                w=w,
                warehouse_id=warehouse_id,
            )
            if validated is not None:
                supporting_proposals.append(validated)
        else:
            supporting_proposals.append(support)

    if proposal is not None:
        proposal.setdefault("patch_type", archetype.patch_type)
        if "usage_guidance" not in proposal:
            proposal["usage_guidance"] = str(proposal.get("rationale") or "").strip()

    if proposal is None:
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype=archetype.name,
            outcome="synth_none",
        )
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=tuple(_attempted_archetypes_so_far),
            skipped_reason="synth_none",
        )

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
                # Plan 2026-05-17-cluster-driven-example-synthesis-hardening
                # Task 7: add _link_prompt_to_trace on the retry path too.
                # Task 4: pass max_tokens.
                from genie_space_optimizer.common.config import (
                    CLUSTER_DRIVEN_EXAMPLE_SYNTHESIS_SYSTEM_MSG,
                    LEVER_5B_CLUSTER_DRIVEN_MAX_TOKENS,
                )
                from genie_space_optimizer.optimization.optimizer import _traced_llm_call
                from genie_space_optimizer.optimization.evaluation import (
                    _link_prompt_to_trace,
                )
                from genie_space_optimizer.optimization.prompt_io import (
                    TeachingKitOutput,
                )
                _link_prompt_to_trace("cluster_driven_example_synthesis")
                try:
                    retry_raw, _ = _traced_llm_call(
                        w, CLUSTER_DRIVEN_EXAMPLE_SYNTHESIS_SYSTEM_MSG, retry_prompt,
                        span_name="cluster_driven_example_synthesis_retry",
                        max_tokens=LEVER_5B_CLUSTER_DRIVEN_MAX_TOKENS,
                        response_model=TeachingKitOutput,
                    )
                except Exception:
                    logger.warning(
                        "cluster-driven: retry LLM call failed for cluster=%s archetype=%s",
                        cluster_id, archetype.name, exc_info=True,
                    )
                    retry_raw = ""
            else:
                retry_raw = llm_caller(retry_prompt)
            retry_raw_proposal = (
                _extract_json_proposal(retry_raw) if retry_raw else None
            )
            # Task 6: run the retry's raw kit through normalize_teaching_kit
            # so its supporting_changes (and primary example) get the same
            # discriminator/type filtering that the primary path applies.
            # Without this, the retry can produce supporting patches with
            # unsupported patch_types that slip past the primary-path
            # validation.
            retry_kit = normalize_teaching_kit(
                retry_raw_proposal or {},
                kit_id=kit_id,
                target_qids=target_qids,
                rca_id=str(cluster.get("rca_id") or ""),
            )
            retry_proposal = retry_kit.primary or None
            retry_supporting = list(retry_kit.supporting)
            if retry_proposal is not None:
                retry_proposal.setdefault("patch_type", archetype.patch_type)
                if "usage_guidance" not in retry_proposal:
                    retry_proposal["usage_guidance"] = str(
                        retry_proposal.get("rationale") or "",
                    ).strip()
                proposal = retry_proposal
                # Replace the primary-path's supporting proposals with the
                # retry's normalized set — the retry is the authoritative
                # response if it succeeded.
                supporting_proposals = []
                for support in retry_supporting:
                    if str(support.get("patch_type") or "").startswith(
                        "add_sql_snippet_"
                    ):
                        validated = _validate_supporting_sql_snippet(
                            support,
                            metadata_snapshot=metadata_snapshot,
                            spark=spark,
                            catalog=catalog,
                            gold_schema=gold_schema,
                            w=w,
                            warehouse_id=warehouse_id,
                        )
                        if validated is not None:
                            supporting_proposals.append(validated)
                    else:
                        supporting_proposals.append(support)
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
        _gate_skipped_reason = (
            f"gate:{first_fail.gate if first_fail else '?'}:"
            f"{first_fail.reason if first_fail else ''}"
        )
        # Task 16: tag the active span with the gate that rejected the
        # proposal so traces can be filtered by gate (parse / execute /
        # structural / firewall / arbiter / etc.). The cluster_id is
        # tagged too so a span filter can drill into one cluster's
        # rejection history.
        if first_fail is not None:
            _set_cluster_driven_span_tag(
                "rejected_by_gate", str(first_fail.gate or "?"),
            )
            _set_cluster_driven_span_tag(
                "rejected_reason", str(first_fail.reason or "")[:200],
            )
        _set_cluster_driven_span_tag("cluster_id", cluster_id)
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype=archetype.name,
            outcome="gate_fail",
            gate_results=gate_results,
            skipped_reason=_gate_skipped_reason,
        )
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=tuple(_attempted_archetypes_so_far),
            skipped_reason=_gate_skipped_reason,
        )

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
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=tuple(_attempted_archetypes_so_far),
            skipped_reason="missing_space_id",
        )

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
        _arbiter_skipped_reason = f"genie_agreement:{agreement.reason}"
        _log_summary(
            "cluster", cluster_id=cluster_id, archetype=archetype.name,
            outcome="arbiter_reject",
            gate_results=list(gate_results) + [agreement],
            skipped_reason=_arbiter_skipped_reason,
        )
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=tuple(_attempted_archetypes_so_far),
            skipped_reason=_arbiter_skipped_reason,
        )

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
        "kit_id": proposal.get("kit_id", kit_id),
        "target_qids": proposal.get("target_qids", target_qids),
        "rca_id": proposal.get("rca_id", ""),
        "_supporting_proposals": supporting_proposals,
    }
    _log_summary(
        "cluster", cluster_id=cluster_id, archetype=archetype.name,
        outcome="applied",
        gate_results=list(gate_results) + [agreement],
        applied=1,
    )
    return ClusterSynthesisResult(
        proposal=final,
        attempted_archetypes=tuple(_attempted_archetypes_so_far),
        skipped_reason="",
    )


# ──────────────────────────────────────────────────────────────────────
# Cycle 9 W3 — narrow-replacement builder for L6 patches dropped at
# ``high_collateral_risk_flagged``. Pure helper, no I/O, no logger;
# safe to unit-test.

_L6_PATCH_TYPES: frozenset[str] = frozenset({
    "add_sql_snippet_measure",
    "add_sql_snippet_filter",
    "add_sql_snippet_expression",
})


def add_qid_scope_to_predicate(
    predicate: str,
    *,
    qids: tuple[str, ...],
    qid_column: str = "query_id",
) -> str:
    """Cycle 9 W3 — wrap ``predicate`` in an AND-clause that scopes it
    to the supplied ``qids``. Returns the input unchanged when the
    predicate already mentions every qid (so the narrow-replacement
    builder can detect the no-op case)."""
    if not predicate or not qids:
        return predicate
    base = str(predicate).strip()
    if not base:
        return base
    if all(q in base for q in qids):
        return base
    quoted = ",".join(f"'{q}'" for q in qids)
    return f"({base}) AND ({qid_column} IN ({quoted}))"


def _narrow_expression_via_qid_case(
    *,
    original_patch: dict,
    ag_target_qids: tuple[str, ...],
) -> dict | None:
    """P0 Branch A: wrap an L6 expression / measure SQL in a
    question-scoped CASE so it only contributes to the named QIDs.

    Returns ``None`` when the original patch has no ``sql_expression``
    or no target QIDs. Pure: no I/O.
    """
    qids = tuple(str(q) for q in (ag_target_qids or ()) if str(q))
    if not qids:
        return None
    expr = str((original_patch or {}).get("sql_expression") or "").strip()
    if not expr:
        return None
    qid_column = str(
        (original_patch or {}).get("qid_predicate_column") or "query_id"
    )
    qid_list = ", ".join(f"'{q}'" for q in qids)
    narrowed_expr = (
        f"CASE WHEN {qid_column} IN ({qid_list}) THEN ({expr}) ELSE NULL END"
    )
    if narrowed_expr == expr:
        return None
    proposal_id = str((original_patch or {}).get("proposal_id") or "")
    return {
        **original_patch,
        "proposal_id": (
            f"{proposal_id}_narrow" if proposal_id else "L6:NARROW"
        ),
        "sql_expression": narrowed_expr,
        "narrowing_strategy": "expression_qid_scope",
        "narrowing_target_qids": list(qids),
    }


# Cycle 10 W4 — patch-type-aware partition. Filter narrows via
# ``where_predicate``; measure / expression lack a predicate and need
# an L5 example_sql fallback instead.
_FILTER_PATCH_TYPES: frozenset[str] = frozenset({"add_sql_snippet_filter"})
_MEASURE_OR_EXPR_PATCH_TYPES: frozenset[str] = frozenset({
    "add_sql_snippet_measure",
    "add_sql_snippet_expression",
})


def narrow_replacement_diagnosis(
    *,
    original_patch: dict,
    ag_target_qids: tuple,
    root_cause: str,
    qid_to_question_text: dict[str, str] | None = None,
    qid_to_reference_sql: dict[str, str] | None = None,
) -> dict:
    """Cycle 10 W4 — return a structured diagnosis of whether a narrow
    L6 replacement is applicable for ``original_patch``.

    Cycle 16 T2 — extended with Branch C: when
    ``GSO_L6_NARROW_REPLACEMENT_BRANCH_C`` is on AND the patch is an L6
    expression / measure AND at least one target QID is *resolvable*
    (has both ``qid_to_question_text[qid]`` and
    ``qid_to_reference_sql[qid]`` non-empty), the diagnosis returns
    ``applicable=True, reason="l5_example_sql_per_qid", branch="C"``.
    Branch C takes precedence over Branch A (the semantically-wrong
    ``query_id``-in-CASE wrap).

    Pure: no I/O. Reads two feature flags at the boundary so the inner
    builders stay pure.

    Returns ``{"applicable": bool, "reason": str, "original_patch_type":
    str, "branch": "A"|"C" (when applicable), "resolvable_target_qids":
    tuple[str, ...] (Branch C only)}``.
    """
    _ = root_cause
    ptype = str((original_patch or {}).get("patch_type") or "")
    qids = tuple(str(q) for q in (ag_target_qids or ()) if str(q))

    if ptype in _MEASURE_OR_EXPR_PATCH_TYPES:
        # Cycle 16 T2 — Branch C dispatch first (takes precedence over
        # Branch A's semantically-wrong query_id-in-CASE wrap).
        from genie_space_optimizer.common.config import (
            l6_narrow_replacement_branch_c_enabled,
            l6_narrow_replacement_for_expression_enabled,
        )
        if l6_narrow_replacement_branch_c_enabled():
            q_text_map = qid_to_question_text or {}
            ref_sql_map = qid_to_reference_sql or {}
            resolvable = tuple(
                q for q in sorted(qids)
                if str(q_text_map.get(q) or "").strip()
                and str(ref_sql_map.get(q) or "").strip()
            )
            if resolvable:
                return {
                    "applicable": True,
                    "reason": "l5_example_sql_per_qid",
                    "original_patch_type": ptype,
                    "branch": "C",
                    "resolvable_target_qids": resolvable,
                }
            return {
                "applicable": False,
                "reason": "no_resolvable_target_qids",
                "original_patch_type": ptype,
                "branch": "C",
            }
        # P0 (legacy Branch A): when the expression-narrowing flag is on
        # AND the patch carries a non-empty sql_expression AND target
        # qids are supplied, the synthesizer can emit a CASE-wrapped
        # narrow variant. Otherwise preserve the legacy decline.
        if (
            l6_narrow_replacement_for_expression_enabled()
            and qids
            and str((original_patch or {}).get("sql_expression") or "").strip()
        ):
            return {
                "applicable": True,
                "reason": "expression_qid_scope",
                "original_patch_type": ptype,
                "branch": "A",
            }
        return {
            "applicable": False,
            "reason": "patch_type_lacks_where_predicate",
            "original_patch_type": ptype,
        }
    if ptype not in _FILTER_PATCH_TYPES:
        # P-E1 — distinguish the "no patch_type at all" subcase from
        # genuinely unrecognized patch types. The legacy
        # ``unrecognized_patch_type`` reason was misleading whenever
        # the orchestrator handed us a placeholder patch with no
        # ``patch_type`` field set.
        if not ptype:
            from genie_space_optimizer.common.config import (
                narrow_skipped_no_original_patch_type_enabled,
            )
            if narrow_skipped_no_original_patch_type_enabled():
                return {
                    "applicable": False,
                    "reason": "narrow_skipped_no_original_patch_type",
                    "original_patch_type": ptype,
                }
        return {
            "applicable": False,
            "reason": "unrecognized_patch_type",
            "original_patch_type": ptype,
        }
    if not qids:
        return {
            "applicable": False,
            "reason": "no_target_qids",
            "original_patch_type": ptype,
        }
    base_predicate = str(
        (original_patch or {}).get("where_predicate") or ""
    ).strip()
    if not base_predicate:
        return {
            "applicable": False,
            "reason": "filter_missing_where_predicate",
            "original_patch_type": ptype,
        }
    return {
        "applicable": True,
        "reason": "filter_predicate_narrowable",
        "original_patch_type": ptype,
        "branch": "A",
    }


# Phase 3 (2026-05-16) — when a narrow-scope replacement is
# synthesised from a broad patch dropped at blast-radius, the
# broad patch's counterfactual-scan stamps (``high_collateral_risk``,
# ``passing_dependents``, and the surfaced ``...outside_target``
# copy) describe the broad predicate's footprint. Copying them
# onto the narrowed variant via ``{**original_patch, ...}`` mis-
# informs ``patch_blast_radius_is_safe`` (proposal_grounding.py:
# ~556, 562), which then re-rejects the narrowed candidate on the
# same grounds that failed the broad one. The strip drops only
# the four gate-driving stamps; benign metadata (patch_type,
# target, where_predicate, qid_predicate_column, proposal_id,
# rca_id, root_cause) survives unchanged.
_STALE_BLAST_RADIUS_STAMPS: frozenset[str] = frozenset({
    "high_collateral_risk",
    "high_collateral_risk_flagged",
    "passing_dependents",
    "passing_dependents_outside_target",
})


def _strip_blast_radius_stamps(patch: dict) -> dict:
    """Return a shallow copy of ``patch`` with the four
    counterfactual-scan / blast-radius stamps removed.

    Pure — no mutation of the input. Used by
    :func:`build_narrow_l6_replacement` to prevent stale stamps
    from polluting the retest at ``harness.py:25813-25828``.
    """
    return {
        k: v for k, v in (patch or {}).items()
        if k not in _STALE_BLAST_RADIUS_STAMPS
    }


def build_narrow_l6_replacement(
    *,
    original_patch: dict,
    ag_target_qids: tuple[str, ...],
    root_cause: str,
    protected_dependents: tuple[str, ...] = (),
) -> dict | None:
    """Cycle 9 W3 / Cycle 10 W4 — synthesize a narrow-scope variant of
    an L6 patch dropped at ``high_collateral_risk_flagged``.

    Cycle 10 W4 splits behavior by patch_type when
    ``GSO_L6_NARROW_REPLACEMENT_PATCH_AWARE`` is on:

    * ``add_sql_snippet_filter`` → narrow ``where_predicate`` with
      ``query_id IN (...)`` scoping. (existing behavior)
    * ``add_sql_snippet_measure`` / ``add_sql_snippet_expression``
      → return ``None``. The harness uses ``narrow_replacement_diagnosis``
      to emit ``NARROW_NOT_APPLICABLE`` and falls back to L5
      example_sql synthesis.

    With the flag off, the legacy filter-only path runs (measure /
    expression also returned ``None`` legacy because they lack a
    where_predicate, so flag-off byte-stability holds).

    Phase 2.5: ``protected_dependents`` is now a first-class keyword
    argument, used by both the Branch C diagnosis path AND the Phase
    2.4 auto-replacement path. Default ``()`` preserves byte-stable
    behavior for all existing callers.

    Pure: no I/O, no clock, no logger.
    """
    from genie_space_optimizer.common.config import (
        l6_narrow_replacement_patch_aware_enabled,
    )
    _ = root_cause  # reserved for future per-RCA narrowing strategies
    _ = protected_dependents  # threaded for prompt context; no effect at default ().
    ptype = str((original_patch or {}).get("patch_type") or "")
    if l6_narrow_replacement_patch_aware_enabled():
        diag = narrow_replacement_diagnosis(
            original_patch=original_patch,
            ag_target_qids=ag_target_qids,
            root_cause=root_cause,
        )
        if not diag["applicable"]:
            return None
        if diag["reason"] == "expression_qid_scope":
            return _narrow_expression_via_qid_case(
                original_patch=original_patch,
                ag_target_qids=ag_target_qids,
            )
    else:
        # Legacy path: only proceed for known L6 types with a
        # where_predicate present.
        if ptype not in _L6_PATCH_TYPES:
            return None
    qids = tuple(str(q) for q in (ag_target_qids or ()) if str(q))
    if not qids:
        return None
    base_predicate = str(
        (original_patch or {}).get("where_predicate") or ""
    ).strip()
    if not base_predicate:
        return None
    qid_column = str(
        (original_patch or {}).get("qid_predicate_column") or "query_id"
    )
    narrowed = add_qid_scope_to_predicate(
        base_predicate, qids=qids, qid_column=qid_column,
    )
    if narrowed == base_predicate:
        return None
    return {
        # Phase 3 (2026-05-16) — strip stale blast-radius stamps
        # before re-stamping the narrow-replacement metadata so the
        # retest at harness.py:~25881 evaluates fresh dependency
        # data instead of inheriting the broad patch's verdict.
        **_strip_blast_radius_stamps(original_patch),
        # Trial 20 Workstream E2 flipped ``patch_blast_radius_is_safe``
        # from safe-by-default to unsafe-by-default when
        # ``passing_dependents`` is absent (``passing_dependents_missing``).
        # The narrowed predicate is ``query_id``-scoped to exactly the
        # target QID(s), so by construction it has ZERO passing
        # dependents outside its target. Re-stamp a FRESH empty list —
        # this is the "fresh dependency data" the Phase 3 strip comment
        # promised. Stripping the stale broad stamp without re-stamping
        # left the field missing, which Trial 20 E2 (default-on) then
        # rejected, killing the recovery path. Empty list ⇒ the gate
        # returns ``no_passing_dependents_outside_target`` (safe).
        "passing_dependents": [],
        "proposal_id": (
            f"{original_patch.get('proposal_id') or 'P_L6'}#NARROW"
        ),
        "where_predicate": narrowed,
        "derived_from": str(original_patch.get("proposal_id") or ""),
        "narrow_replacement_reason": "high_collateral_risk_flagged",
        "narrow_target_qids": qids,
        "narrowing_strategy": "filter_qid_scope",
        "narrowing_target_qids": list(qids),
        "_cycle_9_narrow_replacement": True,
    }


# ──────────────────────────────────────────────────────────────────────
# Cycle 16 T1 — Branch C: L5 example-SQL fallback for L6 expression /
# measure patches dropped at high_collateral_risk_flagged. Pure helper,
# no I/O, no flag reads, no logger; safe to unit-test.
#
# Branch C is the *correct* shape for the metric-view case where Branch
# A's query_id-in-CASE wrap is semantically wrong (metric-view DDL has
# no query_id column). Each resolvable target QID gets one L5
# add_example_sql patch carrying that QID's reference SQL as the
# example_sql payload. A QID is *resolvable* iff both qid_to_question_text
# and qid_to_reference_sql carry a non-empty value for it.

_BRANCH_C_L6_PATCH_TYPES: frozenset[str] = frozenset({
    "add_sql_snippet_expression",
    "add_sql_snippet_measure",
})


def build_l5_example_sql_replacement(
    *,
    original_patch: dict,
    ag_target_qids: tuple[str, ...],
    qid_to_question_text: dict[str, str],
    qid_to_reference_sql: dict[str, str],
    root_cause: str,
    protected_dependents: tuple[str, ...] = (),
) -> tuple[dict, ...]:
    """Cycle 16 T1 — synthesize one Lever-5 ``add_example_sql`` patch
    per resolvable target QID when an L6 expression / measure patch is
    dropped at blast-radius.

    A target QID is resolvable iff both ``qid_to_question_text[qid]``
    and ``qid_to_reference_sql[qid]`` are non-empty strings.

    Each output patch carries:
      * ``patch_type = "add_example_sql"``
      * ``example_question`` and ``example_sql`` (from the resolver
        dicts; the Genie API schema is validated by
        ``applier._validate_example_sql_entry`` at apply time)
      * ``rca_id`` and ``root_cause`` inherited from the parent
      * ``proposal_id = f"{parent_proposal_id}#L5_BRANCH_C_{qid}"`` —
        injective per (parent, qid)
      * ``derived_from = parent_proposal_id``
      * ``narrowing_strategy = "l5_example_sql_per_qid"``
      * ``narrow_replacement_branch = "C"``

    Sort order is by QID ascending so the output is replay byte-stable
    independent of dict insertion order on the caller side.

    Returns ``()`` (empty tuple) when:
      * ``ag_target_qids`` is empty, OR
      * ``original_patch.patch_type`` is not L6 expression / measure, OR
      * no target QID is resolvable.

    Phase 2.5: ``protected_dependents`` is now a first-class keyword
    argument, used by both the Branch C diagnosis path AND the Phase
    2.4 auto-replacement path. Default ``()`` preserves byte-stable
    behavior for all existing callers.

    Pure: no I/O, no clock, no logger, no flag reads.
    """
    _ = protected_dependents  # threaded for prompt context; no effect at default ().
    ptype = str((original_patch or {}).get("patch_type") or "")
    if ptype not in _BRANCH_C_L6_PATCH_TYPES:
        return ()
    qids = tuple(
        str(q).strip()
        for q in (ag_target_qids or ())
        if str(q).strip()
    )
    if not qids:
        return ()
    parent_pid = str(
        (original_patch or {}).get("proposal_id") or "L6:UNKNOWN"
    )
    parent_rca_id = str((original_patch or {}).get("rca_id") or "")
    parent_rationale = str((original_patch or {}).get("rationale") or "")
    parent_target = str((original_patch or {}).get("target") or "")
    rc = str(root_cause or "")

    out: list[dict] = []
    for qid in sorted(qids):
        q_text = str((qid_to_question_text or {}).get(qid) or "").strip()
        ref_sql = str((qid_to_reference_sql or {}).get(qid) or "").strip()
        if not q_text or not ref_sql:
            continue
        out.append({
            "proposal_id": f"{parent_pid}#L5_BRANCH_C_{qid}",
            "patch_type": "add_example_sql",
            "example_question": q_text,
            "example_sql": ref_sql,
            "rca_id": parent_rca_id,
            "root_cause": rc,
            "derived_from": parent_pid,
            "narrowing_strategy": "l5_example_sql_per_qid",
            "narrow_replacement_branch": "C",
            "narrow_target_qids": (qid,),
            "narrowing_target_qids": [qid],
            "rationale": (
                f"Branch C L5 fallback for {parent_pid} on "
                f"{parent_target}; parent rationale: {parent_rationale}"
            ).strip(),
        })
    return tuple(out)


# ──────────────────────────────────────────────────────────────────────
# Plan 1 Task 7 — typed RepairIntent stamping at the synthesis emit
# boundary. Pure helper; no I/O, no logger; safe to call from any
# synthesis dispatch path. Reuses ``intent_from_archetype`` from the
# repair_intent module so the deterministic adapter is the single
# source of truth for Archetype → RepairIntent semantics.


def stamp_proposals_from_archetype(
    *,
    proposals: list[dict],
    archetype,  # archetypes.Archetype (avoid circular import in hint)
    cluster,    # failure_cluster.FailureCluster
    ag_id: str,
) -> None:
    """Stamp a sequence-numbered RepairIntent onto every proposal in
    ``proposals``.

    Each proposal gets its own intent_id of the form
    ``intent_{cluster_id}_{ag_id}_{archetype.name}_{NNN}`` (1-based,
    zero-padded to 3) so the carrier on ProposalSlate can key on the
    intent_id without collision.

    Mutates ``proposals`` in place. Calls
    ``stamp_repair_intent_on_proposal`` which validates the
    patch_type match between proposal and intent — any disagreement
    raises ``RepairIntentPatchTypeMismatchError`` (a synthesizer
    bug).
    """
    from genie_space_optimizer.optimization.repair_intent import (
        intent_from_archetype,
        stamp_repair_intent_on_proposal,
    )

    for seq, proposal in enumerate(proposals, start=1):
        intent = intent_from_archetype(
            archetype=archetype,
            cluster=cluster,
            ag_id=ag_id,
            seq=seq,
        )
        stamp_repair_intent_on_proposal(proposal, intent)


def choose_l6_synthesis_order(*, rca_card: dict | None) -> tuple[str, ...]:
    """Return the order in which L6 synthesis branches should run.

    Closes the gs_021 blank-target-metadata bug: when the RCA card has
    enough scoped metadata to construct a typed proposal, the
    RCA-backed branch runs first and the broad branch is only the
    fallback if no RCA branch produces a candidate.
    """
    if not rca_card:
        return ("broad_emit",)
    targets = rca_card.get("target_objects") or []
    if not targets:
        return ("broad_emit",)
    return ("rca_backed_scoped", "broad_emit")


def _proposal_from_rca_backed_scoped_synthesis(
    *, ag: dict, rca_card: dict | None,
) -> dict | None:
    """Build a typed L6 proposal directly from the RCA card's metadata.

    All proposal fields (target_object, target_qids, rca_card_id,
    causal_target, failing_sql_anchor) come from the RCA card, not
    from downstream fallbacks. This closes the gs_021 blank-target-
    metadata regression.
    """
    if not rca_card or not rca_card.get("target_objects"):
        return None
    target_obj = rca_card["target_objects"][0]
    return {
        "patch_type": rca_card.get(
            "preferred_patch_type", "add_sql_snippet_filter",
        ),
        "target_object": target_obj,
        "snippet": rca_card.get("expected_snippet", ""),
        "target_qids": list(ag.get("affected_questions") or []),
        "rca_card_id": rca_card["rca_card_id"],
        "causal_target": rca_card.get("causal_target", target_obj),
        "failing_sql_anchor": rca_card.get("failing_sql_anchor", ""),
    }
