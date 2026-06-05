"""Trial 21 W2 — Proposal Slate Compiler (the Evidence Actuator).

This module is the *single* decision boundary between Stage 3 raw
``RepairProposals`` and the applier. Before Trial 21, six fragmented
P4 detectors each owned one inline check at six different call sites
and each one operated as ``observe_only=true`` — the marker fired and
the runtime proceeded regardless of verdict.

Trial 21 collapses those six fragments to one pipeline with one verb:
**drop with typed reason**. Each check below calls the existing P4
helper but converts its verdict to a drop. The compiler returns the
surviving slate, the dropped proposals (each tagged with a typed
:class:`DropReason`), per-drop typed feedback the next iteration's
Stage 3 prompt can consume, and the markers the postmortem replay
fixture asserts on.

Check order (the plan's seven steps):

1. **Prompt budget already satisfied** — the cluster's prompt sizer
   verdict must report ``over_cap=false`` AND ``sub_cluster_split_
   needed=false``. If either is true, every proposal in that cluster
   drops with :attr:`DropReason.PROMPT_SPLIT_REQUIRED`. W3 supplies
   the sized verdict; until then this check is a no-op.
2. **Metadata targets resolve** — via
   :func:`metadata_target_resolver.resolve_metadata_patch_target`.
   Unresolvable → :attr:`DropReason.UNRESOLVABLE_TARGET`. W7 closes
   the resolver false-positive case.
3. **SQL snippets validate** — via
   :func:`producer_snippet_validator.validate_and_stamp_snippet_patch_body`.
   Declined → :attr:`DropReason.SNIPPET_INVALID`. W4 wires this at
   the producer (here) and as PHASE3_REGISTRY defense in depth.
4. **Diagnosis evidence sufficient for the lane** — via per-family
   asset requirements from
   :func:`repair_diagnosis.required_assets_for_patch_family`. Missing
   assets → :attr:`DropReason.MISSING_IMPLICATED_ASSETS` (or
   :attr:`DropReason.UNJUSTIFIED_SINGLE_LEVER` for
   ``add_instruction``). W6 lands the asset gate.
5. **Mechanism covers the declared behavior delta** — via
   :func:`mechanism_coverage.check_mechanism_coverage`. Uncovered →
   :attr:`DropReason.UNCOVERED_MECHANISM`. W5 fixes the RCA-to-
   category bridge.
6. **(qid, behavior_delta_fingerprint, patch_mechanism) not already
   failed** — via the fingerprint memory in :func:`admission_gate`.
   Repeat → :attr:`DropReason.REPEATED_FAILED_MECHANISM`. W5 plumbs
   ``prior_mechanism_attempts`` from the kept_insufficient ledger.
7. **Bundle invariants** — single-lever proposals with no
   justification, or bundles that violate the Trial 20 emission
   contract. Drop with :attr:`DropReason.UNJUSTIFIED_SINGLE_LEVER`
   or :attr:`DropReason.BUNDLE_INVARIANT_VIOLATED`.

The compiler degrades gracefully: when a check's input is absent
from the runtime context (e.g. W3 hasn't landed yet so no prompt
size verdict is plumbed), the check is skipped without raising. This
lets W3-W9 land independently against the W1 fixture replay.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Iterable, Mapping, Sequence

from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason


__all__ = (
    "DropReason",
    "TypedFeedback",
    "SlateCompilerContext",
    "SlateCompilerResult",
    "compile_slate",
    "drop_reason_to_terminal_reason",
)


# ---------------------------------------------------------------------
# Typed taxonomy
# ---------------------------------------------------------------------


class DropReason(StrEnum):
    """Closed vocabulary for Actuator-side proposal drops.

    Every drop the Actuator emits MUST pick one of these values. The
    postmortem-replay regression suite (Trial 21 W1) asserts the
    presence of specific drop reasons on the two production fixtures.
    """

    PROMPT_SPLIT_REQUIRED = "prompt_split_required"
    UNRESOLVABLE_TARGET = "unresolvable_target"
    SNIPPET_INVALID = "snippet_invalid"
    MISSING_IMPLICATED_ASSETS = "missing_implicated_assets"
    REPEATED_FAILED_MECHANISM = "repeated_failed_mechanism"
    UNCOVERED_MECHANISM = "uncovered_mechanism"
    UNJUSTIFIED_SINGLE_LEVER = "unjustified_single_lever"
    BUNDLE_INVARIANT_VIOLATED = "bundle_invariant_violated"
    ALL_CANDIDATES_INVALID_SQL = "all_candidates_invalid_sql"
    # Trial 22 W2 — Phase 1.5 cohesion sweep. Trial 20 bundles are
    # atomic units (N sibling proposals sharing bundle_id, one lever
    # each, hypothesis = "these levers TOGETHER fix the behavior
    # delta"). When ANY member is dropped in Phase 1 per-proposal
    # checks, the remaining members of the same bundle are cascaded
    # with this typed reason. The cascade is observable in markers so
    # postmortems show "bundle X dropped because member Y failed Z",
    # not five disjoint drop reasons.
    BUNDLE_MEMBER_DROPPED_CASCADE = "bundle_member_dropped_cascade"


@dataclass(frozen=True, slots=True)
class TypedFeedback:
    """Per-drop feedback that the next iteration's Stage 3 prompt may
    consume so the LLM does not re-propose the same dead end.

    ``proposal_id`` correlates with the dropped ``RepairProposal.intent_id``.
    ``drop_reason`` is the :class:`DropReason` enum value (str-compatible).
    ``feedback_text`` is the human-readable explanation from the underlying
    P4 helper's verdict (or a default sentence keyed off ``drop_reason``).
    """

    proposal_id: str
    drop_reason: str
    feedback_text: str


@dataclass(frozen=True, slots=True)
class SlateCompilerContext:
    """Runtime context the Actuator reads.

    Every field is optional and defaults to an empty container — when
    the corresponding W-item (W3-W9) has not landed yet, the check it
    drives no-ops cleanly. This lets W2 land without depending on the
    other workstreams and lets W3-W9 land independently.
    """

    # Identifiers for marker payloads.
    optimization_run_id: str = ""
    iteration: int = 0
    cluster_id: str = ""

    # W3+C8 — prompt sizer verdict keyed by cluster_id. Each value is a
    # mapping with at least ``over_cap: bool`` and
    # ``sub_cluster_split_needed: bool`` keys; additional fields are
    # passed through to the marker payload.
    prompt_size_verdict_by_cluster: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict
    )

    # W7 — deployed Genie metadata snapshot + space id for the
    # metadata-target resolver. When ``metadata_snapshot`` is empty the
    # resolver step is skipped (see existing P4 helper semantics).
    metadata_snapshot: Mapping[str, Any] = field(default_factory=dict)
    space_id: str = ""

    # W4+C3 — pre-computed snippet validator verdicts keyed by
    # proposal_id (proposal.intent_id). When a verdict is missing for
    # a snippet-patch proposal the check evaluates against the snippet
    # validator directly using the runtime args below.
    snippet_validator_verdict_by_proposal_id: Mapping[str, Mapping[str, Any]] = (
        field(default_factory=dict)
    )
    snippet_validator_runtime: Mapping[str, Any] = field(default_factory=dict)

    # W6+C1 — implicated-asset evidence keyed by proposal_id (and
    # ``justification_by_proposal_id`` for the ``add_instruction``
    # family).
    implicated_assets_by_proposal_id: Mapping[str, Sequence[str]] = field(
        default_factory=dict
    )
    justification_by_proposal_id: Mapping[str, str] = field(default_factory=dict)

    # W5+C2 — kept_insufficient ledger projected into the
    # behavior_delta-fingerprint memory. Each entry is a Mapping with
    # at least ``qid``, ``rca_kind`` (used as the behavior_delta hash
    # input), ``behavioral_diff``, ``patch_type`` and ``selected_lever``.
    prior_mechanism_attempts: Sequence[Mapping[str, Any]] = field(
        default_factory=tuple
    )

    # W5+C5 — RCA classification per QID (Trial 19 LLM-first label).
    # When present, drives the behavior-delta classifier; absent →
    # coverage check uses the empty default and the classifier
    # fail-opens (``OTHER`` category) per the existing P4 behaviour.
    rca_kind_label_by_qid: Mapping[str, str] = field(default_factory=dict)
    behavior_delta_by_qid: Mapping[str, str] = field(default_factory=dict)

    # Trial 24 W24.3 — intent_ids that belong to a valid multi-lever kit
    # (a bundle with >= 2 members spanning >= 2 distinct levers in the
    # union). Populated by :func:`compile_slate` BEFORE Phase 1 runs so
    # the per-proposal ``_check_required_assets`` can waive the
    # instruction-family justification requirement for a kit member —
    # the structural companion lever IS the justification. Empty unless
    # the Trial 24 flag is on; flag-off keeps the field empty and the
    # gate strict (byte-stable rollback). Computing kit membership from
    # the bundle grouping (not the per-proposal lever list) matches how
    # the LLM actually expresses a kit in production: a shared
    # ``bundle_id`` with single-lever members, rather than the full kit
    # list repeated on every member.
    kit_member_intent_ids: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True, slots=True)
class SlateCompilerResult:
    """Return value of :func:`compile_slate`.

    ``surviving_proposals`` are the proposals the Actuator admits to
    the applier. ``dropped_proposals`` carries each dropped proposal
    paired with the :class:`DropReason`. ``typed_feedback_for_retry``
    threads the per-drop feedback into the next iteration's Stage 3
    prompt. ``actuator_markers`` is a tuple of marker payload dicts
    the caller emits via ``run_analysis_contract.marker_line``.
    ``terminal_reason_if_empty`` is the TerminalReason to record when
    every proposal in the input was dropped; ``None`` when the slate
    is non-empty.
    """

    surviving_proposals: tuple[RepairProposal, ...]
    dropped_proposals: tuple[tuple[RepairProposal, DropReason], ...]
    typed_feedback_for_retry: tuple[TypedFeedback, ...]
    actuator_markers: tuple[Mapping[str, Any], ...]
    terminal_reason_if_empty: TerminalReason | None

    # Trial 22 W4 — structured root-cause fields the harness reads to
    # populate IterationTerminalVerdict. Stay closed-vocab on the enum
    # (`terminal_reason_if_empty`); the dynamic root cause goes here.
    @property
    def top_drop_reason(self) -> str:
        """The most load-bearing :class:`DropReason` (by precedence)
        appearing in ``dropped_proposals``, as a wire string. Empty
        when no drops occurred.
        """
        if not self.dropped_proposals:
            return ""
        dropped_set = {reason for _, reason in self.dropped_proposals}
        for candidate in _DROP_REASON_PRECEDENCE:
            if candidate in dropped_set:
                return str(candidate.value)
        return str(self.dropped_proposals[0][1].value)

    @property
    def drop_reason_counts(self) -> dict[str, int]:
        """Per-:class:`DropReason` count across ``dropped_proposals``,
        keyed by the wire-string value."""
        return _count_drop_reasons(self.dropped_proposals)

    @property
    def first_originating_intent_id(self) -> str:
        """The intent_id of the first proposal dropped by the Phase 1
        per-proposal pipeline (the originator of any subsequent
        cascade). Empty when no drops occurred OR every drop came
        from Phase 1.5 / Phase 2."""
        for proposal, reason in self.dropped_proposals:
            if reason != DropReason.BUNDLE_MEMBER_DROPPED_CASCADE:
                return str(proposal.intent_id or "")
        # All drops were cascades — fall back to the first cascaded
        # proposal's intent_id (still informative for postmortems).
        if self.dropped_proposals:
            return str(self.dropped_proposals[0][0].intent_id or "")
        return ""


# ---------------------------------------------------------------------
# DropReason → TerminalReason translation
# ---------------------------------------------------------------------


# Precedence order for empty-slate terminal-reason selection. The first
# DropReason matching the dropped set wins. The order encodes "most
# load-bearing failure first" — prompt overflow is the structural
# blocker, then unresolvable targets, then validator failures, etc.
_DROP_REASON_PRECEDENCE: tuple[DropReason, ...] = (
    # Phase 1 per-proposal failures take precedence — they are the root
    # causes that drive subsequent cascades.
    DropReason.PROMPT_SPLIT_REQUIRED,
    DropReason.UNRESOLVABLE_TARGET,
    DropReason.SNIPPET_INVALID,
    DropReason.MISSING_IMPLICATED_ASSETS,
    DropReason.UNJUSTIFIED_SINGLE_LEVER,
    DropReason.REPEATED_FAILED_MECHANISM,
    DropReason.UNCOVERED_MECHANISM,
    DropReason.BUNDLE_INVARIANT_VIOLATED,
    DropReason.ALL_CANDIDATES_INVALID_SQL,
    # Phase 1.5 cascades are *downstream* of a Phase 1 failure; rank
    # them last so the postmortem-facing ``top_drop_reason`` reports
    # the root cause, not the cascade.
    DropReason.BUNDLE_MEMBER_DROPPED_CASCADE,
)


# Map each DropReason to an existing closed-vocabulary
# :class:`TerminalReason`. The compiler NEVER emits
# ``NO_APPLIED_PATCHES`` for empty slates — that catch-all is exactly
# what the W2 bright-line condition #4 forbids.
_DROP_TO_TERMINAL: Mapping[DropReason, TerminalReason] = {
    DropReason.PROMPT_SPLIT_REQUIRED: TerminalReason.PROPOSAL_GENERATION_EMPTY,
    DropReason.UNRESOLVABLE_TARGET: TerminalReason.APPLYABILITY_REJECTED,
    DropReason.SNIPPET_INVALID: TerminalReason.APPLYABILITY_REJECTED,
    DropReason.MISSING_IMPLICATED_ASSETS: TerminalReason.NO_RCA_GROUND,
    DropReason.UNJUSTIFIED_SINGLE_LEVER: (
        TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY
    ),
    DropReason.REPEATED_FAILED_MECHANISM: TerminalReason.KEPT_INSUFFICIENT,
    DropReason.UNCOVERED_MECHANISM: TerminalReason.NO_STRUCTURAL_CANDIDATE,
    DropReason.BUNDLE_INVARIANT_VIOLATED: TerminalReason.INVARIANT_VIOLATION,
    DropReason.ALL_CANDIDATES_INVALID_SQL: TerminalReason.APPLYABILITY_REJECTED,
    # Trial 22 W2 — cascades surface as INVARIANT_VIOLATION at the
    # enum level (the structured ``originating_drop_reason`` carries
    # the root cause).
    DropReason.BUNDLE_MEMBER_DROPPED_CASCADE: TerminalReason.INVARIANT_VIOLATION,
}


def drop_reason_to_terminal_reason(drop_reason: DropReason) -> TerminalReason:
    """Translate a :class:`DropReason` to its canonical
    :class:`TerminalReason`. Used for the empty-slate
    ``terminal_reason_if_empty`` field on :class:`SlateCompilerResult`.

    The translation is total over the DropReason enum and is pinned by
    unit tests so adding a new DropReason without a TerminalReason
    mapping is caught at test time.
    """
    return _DROP_TO_TERMINAL[drop_reason]


# ---------------------------------------------------------------------
# Per-check helpers — each returns ``DropReason | None``
# ---------------------------------------------------------------------


def _check_prompt_budget(
    proposal: RepairProposal,
    ctx: SlateCompilerContext,
) -> DropReason | None:
    """Step 1 — prompt budget already satisfied at the cluster level.

    Reads the W3 prompt sizer verdict from
    ``ctx.prompt_size_verdict_by_cluster[cluster_id]``. If the verdict
    reports ``over_cap=true`` OR ``sub_cluster_split_needed=true``,
    every proposal in that cluster drops with
    :attr:`DropReason.PROMPT_SPLIT_REQUIRED`.

    Until W3 lands, ``prompt_size_verdict_by_cluster`` is empty and
    this check no-ops. The check is keyed by ``ctx.cluster_id`` (the
    cluster the compile_slate call is scoped to) rather than something
    on the proposal itself because proposals do not carry the
    cluster id.
    """
    verdict = ctx.prompt_size_verdict_by_cluster.get(ctx.cluster_id)
    if not verdict:
        return None
    over_cap = bool(verdict.get("over_cap", False))
    split_needed = bool(verdict.get("sub_cluster_split_needed", False))
    if over_cap or split_needed:
        return DropReason.PROMPT_SPLIT_REQUIRED
    return None


def _check_metadata_target(
    proposal: RepairProposal,
    ctx: SlateCompilerContext,
) -> DropReason | None:
    """Step 2 — metadata-patch target resolves against the snapshot."""
    if not ctx.metadata_snapshot:
        return None
    # Lazy import — keeps the module load graph shallow.
    from genie_space_optimizer.optimization.metadata_target_resolver import (
        METADATA_PATCH_TYPES_WITH_TARGETS,
        resolve_metadata_patch_target,
    )

    wire = str(proposal.patch_type or "").lower()
    if wire not in METADATA_PATCH_TYPES_WITH_TARGETS:
        return None
    body = dict(proposal.patch_body or {})
    verdict = resolve_metadata_patch_target(
        body,
        patch_type_wire=wire,
        metadata_snapshot=ctx.metadata_snapshot,
        space_id=ctx.space_id,
        stamp=False,
    )
    if verdict.outcome == "unresolvable":
        return DropReason.UNRESOLVABLE_TARGET
    return None


def _check_snippet_validator(
    proposal: RepairProposal,
    ctx: SlateCompilerContext,
) -> DropReason | None:
    """Step 3 — SQL snippet validates."""
    proposal_id = str(proposal.intent_id or "")
    verdict = ctx.snippet_validator_verdict_by_proposal_id.get(proposal_id)
    if verdict is None:
        return None
    outcome = str(verdict.get("outcome") or "").lower()
    if outcome == "declined":
        return DropReason.SNIPPET_INVALID
    return None


_METADATA_PATCH_TYPES_REQUIRING_ASSETS = frozenset(
    {
        "add_column_description",
        "update_column_description",
        "add_table_description",
        "add_description",
        "update_description",
        "add_column_synonym",
        "hide_column",
        "unhide_column",
        "rename_column_alias",
    }
)
_SQL_SNIPPET_PATCH_TYPES = frozenset(
    {
        "add_sql_snippet_filter",
        "add_sql_snippet_join",
        "add_sql_snippet_measure",
        "add_sql_snippet_expression",
    }
)


def _check_required_assets(
    proposal: RepairProposal,
    ctx: SlateCompilerContext,
) -> DropReason | None:
    """Step 4 — diagnosis evidence sufficient for the lane.

    Uses :func:`repair_diagnosis.required_assets_for_patch_family` once
    W6 lands. Until then, falls back to a minimum check: column/table-
    description patches must carry at least one implicated asset; the
    ``add_instruction`` family must carry a non-empty justification.

    Skipped when no expectation is wired in ctx for this proposal id
    (defensive — the compiler should not drop on the absence of input).
    """
    proposal_id = str(proposal.intent_id or "")
    wire = str(proposal.patch_type or "").lower()

    # Trial 21 W6+C1 — defensive graceful-degradation: when NEITHER
    # dict has been wired yet, no-op (the harness wires both via
    # synthesize.py when it has the data). Once either dict carries at
    # least one entry the check is active.
    # Trial 22 W6 — emergency rollback escape hatch. When the asset
    # gate is disabled, no-op regardless of wiring.
    if not _asset_gate_enabled():
        return None

    no_assets_wired = not ctx.implicated_assets_by_proposal_id
    no_just_wired = not ctx.justification_by_proposal_id
    if no_assets_wired and no_just_wired:
        return None

    # Look up the patch family's requirement spec.
    from genie_space_optimizer.optimization.repair_diagnosis import (
        required_assets_for_patch_family,
    )

    assets = list(ctx.implicated_assets_by_proposal_id.get(proposal_id) or ())
    just = ctx.justification_by_proposal_id.get(proposal_id, "")
    # We don't track per-proposal sql_shape_delta in the SlateCompiler
    # context (it lives on the RepairDiagnosis the harness owns); pass
    # a hint derived from the patch_body's example_sql /
    # expected_behavioral_change so the gate's "expected_sql_shape"
    # branch fires only when the proposal carries SOME SQL-shape
    # signal. Empty falls back to drop on add_example_sql / friends.
    body = proposal.patch_body or {}
    sql_shape_hint = str(
        body.get("example_sql")
        or body.get("expected_sql_shape")
        or ""
    ).strip()

    # Trial 24 W24.3 — kit-aware waiver. When the proposal declares a
    # >= 2-distinct-lever kit (the same signal the bundle-invariants
    # group check uses), an instruction-family member is justified by
    # its structural companion lever, so the per-proposal
    # ``UNJUSTIFIED_SINGLE_LEVER`` drop must not fire. Flag-gated; the
    # base map carries no Trial 24 RCAs so flag-off is byte-stable.
    in_multi_lever_kit = False
    try:
        from genie_space_optimizer.optimization.trial24_flags import (
            trial24_required_assets_kit_waiver_enabled,
        )

        if trial24_required_assets_kit_waiver_enabled():
            # Bundle-derived membership (computed in compile_slate's
            # pre-scan) is the authoritative signal — it matches how the
            # LLM expresses a kit in production (shared bundle_id with
            # single-lever members). Fall back to the per-proposal lever
            # list for callers that invoke the gate without a pre-scan.
            if proposal_id and proposal_id in ctx.kit_member_intent_ids:
                in_multi_lever_kit = True
            else:
                distinct_levers = {
                    str(s) for s in proposal.effective_selected_levers() if s
                }
                in_multi_lever_kit = len(distinct_levers) >= 2
    except Exception:
        in_multi_lever_kit = False

    verdict = required_assets_for_patch_family(
        patch_type=wire,
        implicated_assets=assets,
        justification=str(just or ""),
        sql_shape_delta=sql_shape_hint,
        in_multi_lever_kit=in_multi_lever_kit,
    )
    if verdict.outcome == "admitted":
        return None
    # Translate the helper's uppercase drop_reason to the closed
    # :class:`DropReason` enum value.
    drop_map = {
        "MISSING_IMPLICATED_ASSETS": DropReason.MISSING_IMPLICATED_ASSETS,
        "UNJUSTIFIED_SINGLE_LEVER": DropReason.UNJUSTIFIED_SINGLE_LEVER,
    }
    return drop_map.get(
        verdict.drop_reason, DropReason.MISSING_IMPLICATED_ASSETS
    )


def _check_mechanism_coverage(
    proposal: RepairProposal,
    ctx: SlateCompilerContext,
) -> DropReason | None:
    """Step 5 — mechanism covers the declared behavior delta."""
    behavior_delta = ""
    for qid in (proposal.target_qids or ()):
        behavior_delta = ctx.behavior_delta_by_qid.get(qid, "") or behavior_delta
        if behavior_delta:
            break
    if not behavior_delta:
        return None

    from genie_space_optimizer.optimization.mechanism_coverage import (
        check_mechanism_coverage,
    )
    from genie_space_optimizer.optimization.patch_mechanism import (
        mechanism_for_patch_type,
    )

    wire = str(proposal.patch_type or "").lower()
    mechanism = mechanism_for_patch_type(wire)
    if mechanism is None:
        return None
    verdict = check_mechanism_coverage(
        behavior_delta=behavior_delta,
        proposed_mechanisms=(mechanism,),
    )
    if verdict.outcome == "uncovered":
        return DropReason.UNCOVERED_MECHANISM
    return None


def _check_mechanism_repeat(
    proposal: RepairProposal,
    ctx: SlateCompilerContext,
) -> DropReason | None:
    """Step 6 — ``(qid, behavior_delta_fingerprint, patch_mechanism)``
    triple not already kept_insufficient.

    Uses a behavior-delta fingerprint (W5) keyed on the RCA-kind label
    + behavioral_diff so semantically-equivalent kept_insufficient
    outcomes match across iterations. Until W5's harness ledger
    plumbing lands, ``prior_mechanism_attempts`` is empty and the
    check no-ops.
    """
    if not ctx.prior_mechanism_attempts:
        return None
    target_qids = set(proposal.target_qids or ())
    if not target_qids:
        return None
    wire = str(proposal.patch_type or "").lower()
    lever = str(proposal.selected_lever or "")

    # Trial 21 W5 — canonical behavior-delta fingerprint key. The hash
    # is taken over the RCA-kind label (Trial 19 LLM-first) plus the
    # behavior-delta free text from the prior attempt; this gives a
    # paraphrase-resistant key for the (qid, behavior, mechanism)
    # triple. When the proposal's behavior_delta is empty (the harness
    # hasn't plumbed it into ``behavior_delta_by_qid`` for this QID
    # yet — happens on the first iteration), we still match on
    # (qid, patch_type, lever) as a defensive fallback so the gs_026
    # repeat scenario remains caught.
    from genie_space_optimizer.optimization.patch_mechanism import (
        behavior_delta_hash,
    )

    current_fingerprint = ""
    for qid in (proposal.target_qids or ()):
        bd = ctx.behavior_delta_by_qid.get(qid, "") or ""
        rk = ctx.rca_kind_label_by_qid.get(qid, "") or ""
        if bd or rk:
            current_fingerprint = behavior_delta_hash(f"{rk}|{bd}")
            break

    for entry in ctx.prior_mechanism_attempts:
        qid = str(entry.get("qid") or "")
        if qid not in target_qids:
            continue
        prior_patch_type = str(entry.get("patch_type") or "").lower()
        prior_lever = str(entry.get("selected_lever") or "")
        if prior_patch_type and prior_patch_type != wire:
            continue
        if prior_lever and lever and prior_lever != lever:
            continue
        # Behavior-delta fingerprint match when both sides carry the
        # signal. When the current proposal lacks a behavior_delta
        # (early-iteration / unplumbed), the (qid, patch_type, lever)
        # match alone is sufficient — the harness has already promoted
        # this triple to ``prior_mechanism_attempts`` because of a
        # previous kept_insufficient outcome.
        if current_fingerprint:
            prior_bd = str(entry.get("behavioral_diff") or "")
            prior_rk = str(entry.get("rca_kind") or "")
            prior_fp = behavior_delta_hash(f"{prior_rk}|{prior_bd}")
            if prior_fp and prior_fp != current_fingerprint:
                continue
        return DropReason.REPEATED_FAILED_MECHANISM
    return None


# ---------------------------------------------------------------------
# Trial 22 W2 — group-aware checks (Phase 1.5 cohesion + Phase 2 group
# invariants). The Trial 21 ``_check_bundle_invariants`` lived inside
# the per-proposal pipeline and evaluated ``proposal.effective_selected_
# levers()`` on each member; production bundles emit N siblings sharing
# bundle_id with one lever each, so every single proposal failed the
# check. Trial 22 splits the pipeline into three phases:
#
#   Phase 1  (per-proposal): checks 1-6 below, runs per proposal.
#   Phase 1.5 (cohesion):    cascade-drop remaining members of any
#                            bundle whose member failed Phase 1.
#   Phase 2  (group):        bundle invariant evaluated on the union
#                            of selected_levers across the bundle.
# ---------------------------------------------------------------------


def _cohesion_sweep_enabled() -> bool:
    """Trial 22 sub-flag — :envvar:`GSO_TRIAL22_BUNDLE_COHESION_SWEEP`.

    Default ON; recognized opt-out values: ``0``, ``false``, ``no``,
    ``off`` (case-insensitive). The cohesion sweep can be disabled
    independently of the group-invariant check for emergency rollback.
    """
    import os

    raw = (os.environ.get("GSO_TRIAL22_BUNDLE_COHESION_SWEEP") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True


def _asset_gate_enabled() -> bool:
    """Trial 22 W6 sub-flag — :envvar:`GSO_TRIAL22_ASSET_GATE`.

    Gates the *active* branch of :func:`_check_required_assets`. When
    OFF the check no-ops even if the harness wired the asset /
    justification dicts, so an emergency rollback restores the
    pre-W6 short-circuit without unplumbing the synthesize call site.
    Default ON; same opt-out vocabulary as the cohesion sweep.
    """
    import os

    raw = (os.environ.get("GSO_TRIAL22_ASSET_GATE") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True


def _group_check_enabled() -> bool:
    """Trial 22 sub-flag — :envvar:`GSO_TRIAL22_BUNDLE_GROUP_CHECK`.

    Default ON; same opt-out vocabulary as the cohesion sweep.
    """
    import os

    raw = (os.environ.get("GSO_TRIAL22_BUNDLE_GROUP_CHECK") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True


def _bundle_dissolve_enabled() -> bool:
    """Trial 22 follow-up sub-flag — :envvar:`GSO_TRIAL22_BUNDLE_DISSOLVE`.

    Gates the singleton-bundle dissolution branch of the Phase 2 group
    check. When a bundle arrives at the group check reduced to a single
    surviving member (its sibling(s) dropped UPSTREAM of the compiler —
    e.g. the Trial 21 producer snippet validator — or in an earlier
    phase), that lone member already passed every per-proposal check and
    is independently applicable. Dissolving it into a solo proposal
    keeps a valid patch alive instead of dropping it as a
    ``BUNDLE_INVARIANT_VIOLATED`` it never caused (the d139 flatline).

    Default ON; same opt-out vocabulary as the cohesion sweep. Flip OFF
    to restore the strict pre-fix behaviour where singleton bundles drop.
    """
    import os

    raw = (os.environ.get("GSO_TRIAL22_BUNDLE_DISSOLVE") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True


def _bundle_distinct_mechanisms(
    members: Sequence[RepairProposal],
) -> set[str]:
    """Follow-on A — distinct coarse mechanism families across a bundle's
    members, derived from each member's ``patch_type``.

    Uses :func:`patch_mechanism.mechanism_for_patch_type` so the kit
    signal is robust to LLM lever mis-tagging: an
    ``add_instruction`` + ``add_sql_snippet_filter`` bundle spans
    ``{INSTRUCTION_TEXT, SQL_SNIPPET}`` regardless of whether the LLM
    tagged both members the same (wrong) lever id. Unclassified
    patch_types (``mechanism_for_patch_type`` returns ``None``) are
    skipped rather than collapsed into a default bucket.
    """
    from genie_space_optimizer.optimization.patch_mechanism import (
        mechanism_for_patch_type,
    )

    mechs: set[str] = set()
    for m in members:
        mech = mechanism_for_patch_type(str(m.patch_type or ""))
        if mech is not None:
            mechs.add(str(mech.value))
    return mechs


def _sweep_bundle_cohesion(
    survivors: Sequence[RepairProposal],
    phase1_drops: Sequence[tuple[RepairProposal, DropReason]],
    ctx: SlateCompilerContext,
) -> tuple[
    list[RepairProposal],
    list[tuple[RepairProposal, DropReason]],
    list[TypedFeedback],
]:
    """Trial 22 W2 Phase 1.5 — cascade-drop the remaining members of
    any bundle whose member was dropped in Phase 1.

    Rationale (atomic-bundle semantics): Trial 20's bundle contract is
    "these N levers TOGETHER fix the behavior delta". When one
    sibling fails its per-proposal check the kit hypothesis is
    compromised; admitting only the surviving members silently
    rewrites the Stage 3 hypothesis. Cascade-drop preserves the
    hypothesis boundary AND gives Stage 3 N+1 a precise retry signal.

    Returns ``(survivors_after_sweep, cascade_drops, cascade_feedback)``.
    Survivors not in any dropped bundle pass through unchanged.
    """
    if not _cohesion_sweep_enabled():
        return list(survivors), [], []
    # Bundle ids whose at-least-one member was dropped in Phase 1.
    # Empty bundle_ids are exempt — solo proposals never cascade.
    dropped_bundle_ids: dict[str, tuple[RepairProposal, DropReason]] = {}
    for proposal, reason in phase1_drops:
        bid = str(proposal.bundle_id or "")
        if not bid:
            continue
        # First Phase-1 drop in a given bundle_id wins as the
        # "originator" for postmortem attribution.
        dropped_bundle_ids.setdefault(bid, (proposal, reason))
    if not dropped_bundle_ids:
        return list(survivors), [], []

    new_survivors: list[RepairProposal] = []
    cascade_drops: list[tuple[RepairProposal, DropReason]] = []
    cascade_feedback: list[TypedFeedback] = []
    for proposal in survivors:
        bid = str(proposal.bundle_id or "")
        if bid and bid in dropped_bundle_ids:
            originator, origin_reason = dropped_bundle_ids[bid]
            cascade_drops.append(
                (proposal, DropReason.BUNDLE_MEMBER_DROPPED_CASCADE)
            )
            cascade_feedback.append(
                TypedFeedback(
                    proposal_id=str(proposal.intent_id or ""),
                    drop_reason=str(
                        DropReason.BUNDLE_MEMBER_DROPPED_CASCADE.value
                    ),
                    feedback_text=(
                        f"Bundle {bid!r} sibling "
                        f"{originator.intent_id!r} was dropped in "
                        f"Phase 1 with reason "
                        f"{str(origin_reason.value)!r}; Trial 20 "
                        "bundles are atomic, so this member is "
                        "cascade-dropped to preserve the Stage 3 "
                        "hypothesis boundary."
                    ),
                )
            )
            continue
        new_survivors.append(proposal)
    return new_survivors, cascade_drops, cascade_feedback


def _check_bundle_invariants_group(
    survivors: Sequence[RepairProposal],
    ctx: SlateCompilerContext,
) -> tuple[
    list[RepairProposal],
    list[tuple[RepairProposal, DropReason]],
    list[TypedFeedback],
    list[tuple[str, RepairProposal]],
    list[tuple[str, RepairProposal]],
]:
    """Trial 22 W2 Phase 2 — slate-level bundle invariants.

    Group survivors by ``bundle_id``; empty ``bundle_id`` proposals
    are exempt (they pass through unchanged). For each group, compute
    the union of selected levers across members. The Trial 20
    bundle-emission contract requires:

      * ``len(group) >= 2`` (bundles must be multi-member); AND
      * ``len(union_levers) >= 2`` (the kit must span >= 2 lever
        families).

    A genuine MULTI-member bundle that fails the lever-union test drops
    EVERY member with :attr:`DropReason.BUNDLE_INVARIANT_VIOLATED`.

    A SINGLETON bundle (``member_count == 1``) is a different animal:
    it was not born invalid — its sibling(s) were dropped UPSTREAM of
    the compiler (Trial 21 producer snippet validator) or in an earlier
    phase, leaving one member that already passed every per-proposal
    Phase 1 check. Dropping it as ``BUNDLE_INVARIANT_VIOLATED`` is what
    reproduced the d139 flatline (live fevm-prashanth finding). When
    :func:`_bundle_dissolve_enabled` is ON (default) the singleton is
    DISSOLVED: its now-meaningless ``bundle_id`` is cleared and the lone
    member proceeds as a solo proposal — exactly how an empty-bundle_id
    proposal already passes Phase 2 untouched. The returned 4th element
    carries ``(original_bundle_id, original_proposal)`` records so the
    caller can emit a ``GSO_TRIAL22_BUNDLE_DISSOLVED_V1`` marker.

    Trial 23 W9 — bundle repair over drop. A GENUINE multi-member bundle
    that fails the lever-union test (a same-lever / cohesion-failing
    bundle: ``member_count >= 2`` but ``len(union_levers) < 2``) used to
    drop EVERY member. But each member already passed every per-proposal
    Phase 1 check and is independently applicable — only the kit
    hypothesis ("these families TOGETHER") is invalid. When
    :func:`trial23_bundle_repair_enabled` is ON the bundle is RECOMPOSED:
    dissolved into its independently-valid solo members instead of
    dropped. The returned 5th element carries the recomposed
    ``(original_bundle_id, original_proposal)`` records so the caller can
    emit a ``GSO_TRIAL23_BUNDLE_RECOMPOSED_V1`` marker.

    Returns ``(new_survivors, drops, feedback, dissolved, recomposed)``.
    """
    if not _group_check_enabled():
        return list(survivors), [], [], [], []
    dissolve_on = _bundle_dissolve_enabled()
    from genie_space_optimizer.optimization.trial23_flags import (
        trial23_bundle_repair_enabled,
    )
    recompose_on = trial23_bundle_repair_enabled()
    from genie_space_optimizer.optimization.trial24_flags import (
        trial24_mechanism_aware_kit_enabled,
    )
    mech_aware_on = trial24_mechanism_aware_kit_enabled()
    new_survivors: list[RepairProposal] = []
    drops: list[tuple[RepairProposal, DropReason]] = []
    feedback: list[TypedFeedback] = []
    dissolved: list[tuple[str, RepairProposal]] = []
    recomposed: list[tuple[str, RepairProposal]] = []

    # Partition survivors into solo (no bundle_id) and bundle groups.
    bundle_groups: dict[str, list[RepairProposal]] = {}
    for proposal in survivors:
        bid = str(proposal.bundle_id or "")
        if not bid:
            new_survivors.append(proposal)
            continue
        bundle_groups.setdefault(bid, []).append(proposal)

    for bid, members in bundle_groups.items():
        union_levers: set[str] = set()
        for member in members:
            kit = member.effective_selected_levers()
            if kit:
                union_levers.update(kit)
            elif member.selected_lever:
                union_levers.add(member.selected_lever)
        union_levers.discard("")
        member_count = len(members)
        # Follow-on A — admit by declared-lever union OR (flag-on) by
        # distinct patch_type-derived mechanism families, so a kit whose
        # members the LLM mis-tagged with the same lever still survives
        # the Phase-2 invariant rather than being recomposed/dropped.
        kit_ok = len(union_levers) >= 2
        if not kit_ok and mech_aware_on:
            kit_ok = len(_bundle_distinct_mechanisms(members)) >= 2
        if member_count >= 2 and kit_ok:
            new_survivors.extend(members)
            continue
        if member_count == 1 and dissolve_on:
            # Singleton bundle — dissolve into a solo proposal rather
            # than drop a valid, independently-applicable member.
            sole = members[0]
            dissolved.append((bid, sole))
            new_survivors.append(replace(sole, bundle_id=""))
            continue
        if member_count >= 2 and recompose_on:
            # Trial 23 W9 — same-lever / cohesion-failing multi-member
            # bundle. The kit hypothesis is invalid, but every member
            # already passed its per-proposal Phase 1 checks and is
            # independently applicable. Recompose: dissolve the bundle
            # and let each member proceed as a solo proposal instead of
            # dropping behaviour-changing patches as a contract
            # violation they can still individually honour.
            for member in members:
                recomposed.append((bid, member))
                new_survivors.append(replace(member, bundle_id=""))
            continue
        # Genuine multi-member contract violation (recompose disabled)
        # or singleton with dissolution disabled — drop every member.
        for member in members:
            drops.append((member, DropReason.BUNDLE_INVARIANT_VIOLATED))
            feedback.append(
                TypedFeedback(
                    proposal_id=str(member.intent_id or ""),
                    drop_reason=str(
                        DropReason.BUNDLE_INVARIANT_VIOLATED.value
                    ),
                    feedback_text=(
                        f"Bundle {bid!r} violates Trial 20 contract: "
                        f"member_count={member_count}, "
                        f"union_levers={sorted(union_levers)}. "
                        "Bundles must carry >= 2 members AND >= 2 "
                        "distinct lever families across the union "
                        "(NOT per individual proposal)."
                    ),
                )
            )
    return new_survivors, drops, feedback, dissolved, recomposed


# Phase 1 — per-proposal pipeline (in plan-prescribed order).
_PIPELINE_PER_PROPOSAL: tuple[
    tuple[
        str,
        "Callable[[RepairProposal, SlateCompilerContext], DropReason | None]",
    ],
    ...,
] = (  # type: ignore[name-defined]
    ("prompt_budget", _check_prompt_budget),
    ("metadata_target", _check_metadata_target),
    ("snippet_validator", _check_snippet_validator),
    ("required_assets", _check_required_assets),
    ("mechanism_coverage", _check_mechanism_coverage),
    ("mechanism_repeat", _check_mechanism_repeat),
)

# Back-compat alias — Trial 21 unit tests imported ``_PIPELINE``.
_PIPELINE = _PIPELINE_PER_PROPOSAL


# ---------------------------------------------------------------------
# Per-DropReason feedback boilerplate
# ---------------------------------------------------------------------


_DEFAULT_FEEDBACK: Mapping[DropReason, str] = {
    DropReason.PROMPT_SPLIT_REQUIRED: (
        "Stage 3 prompt exceeds the 40k cap even after per-segment "
        "slicing; retry with a sub-cluster split (one QID per call)."
    ),
    DropReason.UNRESOLVABLE_TARGET: (
        "Metadata patch target does not exist in the deployed Genie "
        "config; canonicalize the table/column or pick a different "
        "asset that does."
    ),
    DropReason.SNIPPET_INVALID: (
        "SQL snippet failed validate_sql_snippet; the producer rejected "
        "the proposal so the applier never sees it. Try a different "
        "snippet shape or fall back to add_example_sql with a "
        "concrete query."
    ),
    DropReason.MISSING_IMPLICATED_ASSETS: (
        "Patch family requires concrete catalog.schema.table[.column] "
        "assets; the diagnosis did not name any. Sharpen the Stage 1 "
        "prompt to emit implicated_assets or switch lanes."
    ),
    DropReason.REPEATED_FAILED_MECHANISM: (
        "This (qid, behavior_delta_fingerprint, patch_mechanism) triple "
        "was already kept_insufficient on a prior iteration. Switch "
        "mechanism family or pair with at least one additional new "
        "mechanism and emit a non-empty mechanism_change_justification."
    ),
    DropReason.UNCOVERED_MECHANISM: (
        "Proposed mechanism does not cover the declared behavior_delta "
        "category. Either swap to an adequate mechanism or emit a "
        "mechanism_coverage_override_justification with concrete "
        "evidence."
    ),
    DropReason.UNJUSTIFIED_SINGLE_LEVER: (
        "Single-lever instruction proposal must carry a non-empty "
        "justification AND must not repeat a prior kept_insufficient "
        "single-lever signature."
    ),
    DropReason.BUNDLE_INVARIANT_VIOLATED: (
        "Trial 20 bundle-emission contract violated; bundles must "
        "carry effective_selected_levers of length >= 2 evaluated "
        "across the union of bundle members (NOT per individual "
        "proposal)."
    ),
    DropReason.ALL_CANDIDATES_INVALID_SQL: (
        "Every SQL candidate failed validation; abandon the cluster "
        "and pivot to a different repair family."
    ),
    DropReason.BUNDLE_MEMBER_DROPPED_CASCADE: (
        # Cascade feedback is dynamic — :func:`_sweep_bundle_cohesion`
        # overwrites this default with the originating sibling
        # intent_id and its DropReason. This sentence is the fallback
        # used only when the sweep cannot identify the originator.
        "A sibling proposal in the same bundle_id was dropped by a "
        "Phase 1 per-proposal check. Trial 20 bundles are atomic; the "
        "remaining bundle members are cascaded so the slate matches "
        "the original Stage 3 hypothesis exactly."
    ),
}


# ---------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------


def compile_slate(
    raw_proposals: Iterable[RepairProposal],
    runtime_ctx: SlateCompilerContext,
) -> SlateCompilerResult:
    """Run the three-phase pipeline against ``raw_proposals``.

    Phase 1 — per-proposal checks 1-6: short-circuit on first failing
    check; the first failure determines the proposal's :class:`DropReason`.

    Phase 1.5 — bundle cohesion sweep (Trial 22 W2): cascade-drop the
    remaining members of any bundle whose member failed Phase 1.
    Cascades carry :attr:`DropReason.BUNDLE_MEMBER_DROPPED_CASCADE` and
    feedback that names the originating sibling and reason. Trial 20
    bundles are atomic; admitting only the surviving members would
    silently rewrite the Stage 3 hypothesis.

    Phase 2 — slate-level bundle invariants (Trial 22 W2): the
    bundle-emission contract is evaluated on the union of
    selected_levers across the bundle, NOT per individual proposal.
    A bundle with cardinality >= 2 members AND >= 2 distinct levers
    in the union admits all members; otherwise every member drops
    with :attr:`DropReason.BUNDLE_INVARIANT_VIOLATED`.

    Empty-slate ``terminal_reason_if_empty`` is picked deterministically
    by precedence over the encountered :class:`DropReason` values via
    :data:`_DROP_REASON_PRECEDENCE`. Cascades are ranked LAST so the
    top reason reports the root cause, not the cascade.
    """
    drops: list[tuple[RepairProposal, DropReason]] = []
    feedback: list[TypedFeedback] = []
    markers: list[Mapping[str, Any]] = []

    # Materialize once — we iterate the proposals twice (kit pre-scan +
    # Phase 1). ``raw_proposals`` may be a one-shot iterable.
    proposals_list = list(raw_proposals)

    # ── Trial 24 W24.3 — kit-membership pre-scan ──────────────────
    # Before Phase 1's per-proposal gate runs, identify which proposals
    # belong to a valid multi-lever kit so the justification waiver can
    # fire on the instruction member. A kit is a bundle with >= 2
    # members spanning >= 2 distinct levers in the union — the SAME
    # signal the Phase 2 ``_check_bundle_invariants_group`` uses. This
    # is bundle-derived (not per-proposal) because the production LLM
    # emits a shared ``bundle_id`` with single-lever members rather than
    # repeating the full kit list on each member. Flag-gated; flag-off
    # leaves ``kit_member_intent_ids`` empty (byte-stable).
    try:
        from genie_space_optimizer.optimization.trial24_flags import (
            trial24_required_assets_kit_waiver_enabled,
        )

        if (
            trial24_required_assets_kit_waiver_enabled()
            and not runtime_ctx.kit_member_intent_ids
        ):
            _kit_bundles: dict[str, list[RepairProposal]] = {}
            for _p in proposals_list:
                _bid = str(_p.bundle_id or "")
                if _bid:
                    _kit_bundles.setdefault(_bid, []).append(_p)
            from genie_space_optimizer.optimization.trial24_flags import (
                trial24_mechanism_aware_kit_enabled,
            )

            _mech_aware = trial24_mechanism_aware_kit_enabled()
            _kit_ids: set[str] = set()
            for _members in _kit_bundles.values():
                if len(_members) < 2:
                    continue
                _union: set[str] = set()
                for _m in _members:
                    _union.update(
                        str(s) for s in _m.effective_selected_levers() if s
                    )
                # Follow-on A — accept the kit by declared-lever union OR
                # (flag-on) by distinct patch_type-derived mechanisms, so
                # an LLM that mis-tags both members the same lever is
                # still recognised as a multi-mechanism kit.
                _is_kit = len(_union) >= 2
                if not _is_kit and _mech_aware:
                    _is_kit = len(_bundle_distinct_mechanisms(_members)) >= 2
                if _is_kit:
                    for _m in _members:
                        _id = str(_m.intent_id or "")
                        if _id:
                            _kit_ids.add(_id)
            if _kit_ids:
                runtime_ctx = replace(
                    runtime_ctx,
                    kit_member_intent_ids=frozenset(_kit_ids),
                )
    except Exception:
        pass

    # ── Phase 1 — per-proposal checks ─────────────────────────────
    phase1_survivors: list[RepairProposal] = []
    for proposal in proposals_list:
        drop_reason: DropReason | None = None
        failing_check = ""
        for check_name, check_fn in _PIPELINE_PER_PROPOSAL:
            verdict = check_fn(proposal, runtime_ctx)
            if verdict is not None:
                drop_reason = verdict
                failing_check = check_name
                break
        if drop_reason is None:
            phase1_survivors.append(proposal)
            continue
        drops.append((proposal, drop_reason))
        feedback.append(
            TypedFeedback(
                proposal_id=str(proposal.intent_id or ""),
                drop_reason=str(drop_reason.value),
                feedback_text=_DEFAULT_FEEDBACK[drop_reason],
            )
        )
        markers.append(
            {
                "marker": "GSO_SLATE_COMPILER_DECISION_V1",
                "optimization_run_id": runtime_ctx.optimization_run_id,
                "iteration": runtime_ctx.iteration,
                "cluster_id": runtime_ctx.cluster_id,
                "proposal_id": str(proposal.intent_id or ""),
                "qids": list(proposal.target_qids or ()),
                "patch_type": str(proposal.patch_type or ""),
                "drop_reason": str(drop_reason.value),
                "failing_check": failing_check,
                "phase": "per_proposal",
            }
        )

    # ── Phase 1.5 — bundle cohesion sweep ─────────────────────────
    (
        phase15_survivors,
        cascade_drops,
        cascade_feedback,
    ) = _sweep_bundle_cohesion(phase1_survivors, drops, runtime_ctx)
    for proposal, reason in cascade_drops:
        drops.append((proposal, reason))
        # Emit a marker per cascaded proposal so postmortems can link
        # the cascade to its originating sibling.
        # ``_sweep_bundle_cohesion`` already encoded the link in the
        # feedback text; pull the bundle_id and originator out for the
        # structured marker payload.
        bid = str(proposal.bundle_id or "")
        markers.append(
            {
                "marker": "GSO_SLATE_COMPILER_DECISION_V1",
                "optimization_run_id": runtime_ctx.optimization_run_id,
                "iteration": runtime_ctx.iteration,
                "cluster_id": runtime_ctx.cluster_id,
                "proposal_id": str(proposal.intent_id or ""),
                "qids": list(proposal.target_qids or ()),
                "patch_type": str(proposal.patch_type or ""),
                "drop_reason": str(reason.value),
                "failing_check": "bundle_cohesion",
                "phase": "cohesion_sweep",
                "bundle_id": bid,
            }
        )
    feedback.extend(cascade_feedback)

    # ── Phase 2 — group-level bundle invariants ───────────────────
    (
        final_survivors,
        group_drops,
        group_feedback,
        group_dissolved,
        group_recomposed,
    ) = _check_bundle_invariants_group(phase15_survivors, runtime_ctx)
    for bid, proposal in group_recomposed:
        # Trial 23 W9 — a same-lever / cohesion-failing multi-member
        # bundle was RECOMPOSED into independently-valid solo proposals
        # (NOT a drop). Emit an observability marker so postmortems can
        # see the members proceeded instead of dying as a
        # bundle_invariant_violation each could individually honour.
        markers.append(
            {
                "marker": "GSO_TRIAL23_BUNDLE_RECOMPOSED_V1",
                "optimization_run_id": runtime_ctx.optimization_run_id,
                "iteration": runtime_ctx.iteration,
                "cluster_id": runtime_ctx.cluster_id,
                "proposal_id": str(proposal.intent_id or ""),
                "qids": list(proposal.target_qids or ()),
                "patch_type": str(proposal.patch_type or ""),
                "bundle_id": bid,
                "phase": "group",
                "reason": "same_lever_bundle_recomposed_to_solo",
            }
        )
    for bid, proposal in group_dissolved:
        # Trial 22 follow-up — a singleton bundle was dissolved into a
        # solo proposal. Emit an observability marker (NOT a drop) so
        # postmortems can see the lone member proceeded instead of
        # dying as a bundle_invariant_violation it never caused.
        markers.append(
            {
                "marker": "GSO_TRIAL22_BUNDLE_DISSOLVED_V1",
                "optimization_run_id": runtime_ctx.optimization_run_id,
                "iteration": runtime_ctx.iteration,
                "cluster_id": runtime_ctx.cluster_id,
                "proposal_id": str(proposal.intent_id or ""),
                "qids": list(proposal.target_qids or ()),
                "patch_type": str(proposal.patch_type or ""),
                "bundle_id": bid,
                "phase": "group",
                "reason": "singleton_bundle_dissolved_to_solo",
            }
        )
    for proposal, reason in group_drops:
        drops.append((proposal, reason))
        markers.append(
            {
                "marker": "GSO_SLATE_COMPILER_DECISION_V1",
                "optimization_run_id": runtime_ctx.optimization_run_id,
                "iteration": runtime_ctx.iteration,
                "cluster_id": runtime_ctx.cluster_id,
                "proposal_id": str(proposal.intent_id or ""),
                "qids": list(proposal.target_qids or ()),
                "patch_type": str(proposal.patch_type or ""),
                "drop_reason": str(reason.value),
                "failing_check": "bundle_invariants_group",
                "phase": "group",
                "bundle_id": str(proposal.bundle_id or ""),
            }
        )
    feedback.extend(group_feedback)

    # Cluster-scoped summary marker so postmortems can group drops.
    markers.append(
        {
            "marker": "GSO_SLATE_COMPILER_DECISION_V1",
            "optimization_run_id": runtime_ctx.optimization_run_id,
            "iteration": runtime_ctx.iteration,
            "cluster_id": runtime_ctx.cluster_id,
            "survivor_count": len(final_survivors),
            "drop_count": len(drops),
            "drop_reason_counts": _count_drop_reasons(drops),
            "is_summary": True,
        }
    )

    terminal_reason: TerminalReason | None
    if final_survivors:
        terminal_reason = None
    elif drops:
        terminal_reason = _pick_terminal_reason(drops)
    else:
        # No proposals in, no proposals out. The caller decides whether
        # this is a strategist-side terminal (handled outside the
        # Actuator).
        terminal_reason = TerminalReason.PROPOSAL_GENERATION_EMPTY

    return SlateCompilerResult(
        surviving_proposals=tuple(final_survivors),
        dropped_proposals=tuple(drops),
        typed_feedback_for_retry=tuple(feedback),
        actuator_markers=tuple(markers),
        terminal_reason_if_empty=terminal_reason,
    )


def _count_drop_reasons(
    drops: Sequence[tuple[RepairProposal, DropReason]],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for _, reason in drops:
        key = str(reason.value)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _pick_terminal_reason(
    drops: Sequence[tuple[RepairProposal, DropReason]],
) -> TerminalReason:
    """Deterministic terminal-reason picker: first DropReason in
    :data:`_DROP_REASON_PRECEDENCE` that appears in ``drops``."""
    dropped_set = {reason for _, reason in drops}
    for candidate in _DROP_REASON_PRECEDENCE:
        if candidate in dropped_set:
            return drop_reason_to_terminal_reason(candidate)
    # Defensive: every DropReason maps; fall back to the first drop's
    # mapping.
    return drop_reason_to_terminal_reason(drops[0][1])


# ---------------------------------------------------------------------
# Trial 22 W3 — durable retry feedback round-trip
# ---------------------------------------------------------------------


def build_compiler_drop_summary(
    result: SlateCompilerResult,
) -> dict[str, Any]:
    """Trial 22 W3 — project a :class:`SlateCompilerResult` into the
    durable ``compiler_drop_summary`` schema that lives on the
    iteration terminal-state ledger row (NOT only on the transient
    ``ClusterSynthesisResult``).

    Schema (matches the W3 plan contract):

    .. code-block:: python

        {
            "top_drop_reasons": list[str],       # by precedence, deduped
            "drop_reason_counts": dict[str, int],
            "specific_violations": list[dict],   # bundle_id / member_count / union_levers
            "originating_intents": list[str],    # Phase-1 originators (root causes)
        }

    Returns an empty-but-well-formed summary when no drops occurred so
    callers can store it unconditionally.
    """
    counts = result.drop_reason_counts
    # Order top reasons by the global precedence so the most
    # load-bearing root cause leads.
    ordered_present = [
        str(dr.value)
        for dr in _DROP_REASON_PRECEDENCE
        if str(dr.value) in counts
    ]

    specific_violations: list[dict[str, Any]] = []
    originating_intents: list[str] = []
    for proposal, reason in result.dropped_proposals:
        if reason == DropReason.BUNDLE_INVARIANT_VIOLATED:
            kit = proposal.effective_selected_levers()
            specific_violations.append(
                {
                    "bundle_id": str(proposal.bundle_id or ""),
                    "intent_id": str(proposal.intent_id or ""),
                    "member_levers": list(kit),
                }
            )
        if reason != DropReason.BUNDLE_MEMBER_DROPPED_CASCADE:
            # Phase-1 / Phase-2 originators are the actionable root
            # causes; cascades are downstream effects.
            intent_id = str(proposal.intent_id or "")
            if intent_id and intent_id not in originating_intents:
                originating_intents.append(intent_id)

    return {
        "top_drop_reasons": ordered_present,
        "drop_reason_counts": dict(counts),
        "specific_violations": specific_violations,
        "originating_intents": originating_intents,
    }


def render_prior_iteration_drops(
    summary: Mapping[str, Any] | None,
) -> str:
    """Trial 22 W3 — render a ``<prior_iteration_drops>`` prompt
    section from a ``compiler_drop_summary`` so the Stage 3 N+1
    ``_build_request`` can warn the LLM about exactly which proposals
    the compiler dropped last iteration and why.

    Returns the empty string when ``summary`` is falsy or carries no
    drops, so callers can concatenate unconditionally.
    """
    if not summary:
        return ""
    counts = dict(summary.get("drop_reason_counts") or {})
    if not counts:
        return ""
    top_reasons = list(summary.get("top_drop_reasons") or [])
    specific = list(summary.get("specific_violations") or [])
    originating = list(summary.get("originating_intents") or [])

    lines: list[str] = ["<prior_iteration_drops>"]
    lines.append(
        "The slate compiler dropped EVERY proposal you emitted last "
        "iteration. Do NOT re-emit the same shapes. Drop reasons:"
    )
    for reason in top_reasons:
        lines.append(f"  - {reason}: {counts.get(reason, 0)}")
    if specific:
        lines.append("Specific bundle-invariant violations:")
        for v in specific:
            lines.append(
                "  - bundle_id="
                f"{v.get('bundle_id', '')!r} "
                f"member_levers={v.get('member_levers', [])}"
            )
    if originating:
        lines.append(
            "Originating (root-cause) proposal intent_ids: "
            + ", ".join(str(i) for i in originating)
        )
    lines.append("</prior_iteration_drops>")
    return "\n".join(lines)


def build_retry_feedback_marker(
    *,
    iteration: int,
    summary: Mapping[str, Any] | None,
    retry_lever_hints: Sequence[str] = (),
) -> dict[str, Any]:
    """Trial 22 W3 — assemble the ``GSO_TRIAL22_RETRY_FEEDBACK_V1``
    marker payload. ``durability_source`` is pinned so postmortems can
    confirm the feedback was sourced from the durable ledger and not a
    transient cluster result.
    """
    summary = summary or {}
    return {
        "marker": "GSO_TRIAL22_RETRY_FEEDBACK_V1",
        "iteration": int(iteration),
        "top_drop_reasons": list(summary.get("top_drop_reasons") or []),
        "drop_reason_counts": dict(summary.get("drop_reason_counts") or {}),
        "retry_lever_hints": list(retry_lever_hints or ()),
        "durability_source": "iteration_terminal_state_ledger",
    }
