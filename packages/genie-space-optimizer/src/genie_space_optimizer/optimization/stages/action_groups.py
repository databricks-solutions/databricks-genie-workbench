"""Stage 4: Action Group selection (Phase F4).

Wraps the existing decision_emitters.strategist_ag_records producer in
a typed ActionGroupsInput / ActionGroupSlate surface so F5 (proposals)
can read the slate from a stage-aligned dataclass.

F4 is observability-only: per the plan's Reality Check appendix, the
strategist invocation block in harness.py is a non-contiguous sequence
of inline operations (~300-500 LOC), not a function. Lifting it
inside a single F4 gate is high-risk for byte-stability. F4 stands up
the typed surface and STRATEGIST_AG_EMITTED emission entry; the LLM
invocation, constraint filtering, and buffered-AG draining stay in
harness for now and are deferred to a follow-up plan.

C15 Phase 3: adds JsonRoundTrip to ActionGroupsInput / ActionGroupSlate,
and adds the ForbiddenReason / AdmissionVerdict / ForbiddenAG /
AdmissionTrace admission-trace types. When stage_handlers_chunk_b_enabled()
is on, select() populates ActionGroupSlate.admission_trace from the
forbidden-AG set so the postmortem bundle can surface which AGs were
denied and why (forbidden-AG no-op loop observability).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Mapping, Sequence

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.action_group import ActionGroup

from genie_space_optimizer.optimization.decision_emitters import (
    strategist_ag_records,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    AlternativeOption,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


STAGE_KEY: str = "action_group_selection"


# ── C15 Phase 3: Admission trace types ────────────────────────────────


class ForbiddenReason(StrEnum):
    """Why an AG was denied admission to the slate.

    Mirrors the rollback_class vocabulary used by
    ``_compute_forbidden_ag_set`` in harness.py so postmortems can
    cross-reference forbidden-AG denials with the reflection buffer
    without re-parsing raw harness logs.
    """
    CONTENT_REGRESSION = "content_regression"
    NO_PROPOSALS = "no_proposals"
    AG_RETIRED = "ag_retired"
    OTHER = "other"


class AdmissionVerdict(StrEnum):
    ADMITTED = "admitted"
    DENIED = "denied"


@dataclass(frozen=True, slots=True)
class ForbiddenAG(JsonRoundTrip):
    """A single forbidden-AG record supplied to ``ActionGroupsInput``.

    ``ag_id`` matches the ``id`` / ``ag_id`` key on candidate AG dicts.
    ``reason`` is the ``ForbiddenReason`` that caused the denial.
    """
    ag_id: str
    reason: ForbiddenReason

    @classmethod
    def from_json(cls, payload: dict) -> "ForbiddenAG":
        return cls(
            ag_id=str(payload["ag_id"]),
            reason=ForbiddenReason(payload["reason"]),
        )


@dataclass(frozen=True, slots=True)
class AdmissionTrace(JsonRoundTrip):
    """Per-candidate admission verdict for a single AG.

    Populated in ``ActionGroupSlate.admission_trace`` when
    ``stage_handlers_chunk_b_enabled()`` is on.  Empty tuple when
    flag is off (byte-stable with legacy behaviour).
    """
    ag_id: str
    verdict: AdmissionVerdict
    denial_reason: str = ""

    @classmethod
    def from_json(cls, payload: dict) -> "AdmissionTrace":
        return cls(
            ag_id=str(payload["ag_id"]),
            verdict=AdmissionVerdict(payload["verdict"]),
            denial_reason=str(payload.get("denial_reason", "")),
        )


# ── C15 Phase 2 Task 4 — root causes that are inherently question-local.
# A single-question cluster with one of these root causes should be
# fixed with a per-question lever, not a space-wide one.
_QUESTION_SHAPE_ROOT_CAUSES: frozenset[str] = frozenset({
    "plural_top_n_collapse",
    "count_vs_distinct",
    "row_ordering_drift",
    "limit_vs_rank",
})

# Cycle 2 Task 4 — default per-question levers. 3 = example_sql
# (benchmark anchor), 5 = instructions narrowed by question_id.
_PER_QUESTION_PREFERRED_LEVERS: tuple[int, ...] = (3, 5)

# Cycle 2 Task 4 — default space-wide levers when no preference fires.
# Includes 6 (SQL expressions) which is appropriate for multi-
# question patterns.
_DEFAULT_RECOMMENDED_LEVERS: tuple[int, ...] = (3, 5, 6)


# ── Phase 2 P2.2 — KIT_FOR_RCA mandatory-companion map ────────────────
#
# Some RCA kinds are demonstrably under-served by a single lever in
# isolation — postmortems across Trials 17–20 show that
# instruction-only or snippet-only repairs for these diagnoses
# regress to ``target_unchanged`` because the Genie planner needs
# BOTH the structural lever (a SQL snippet, expression, or example)
# AND a complementary metadata lever (a column / table description or
# value-mapping instruction) to compose the right grammar.
#
# This map is consulted by the Stage 3 synthesizer validator (see
# ``stages.synthesize``) AFTER the LLM emits proposals. For every
# proposal whose source diagnosis ``rca_kind`` matches a key here,
# the validator HARD-REJECTS the proposal when
# ``proposal.effective_selected_levers()`` returns a single-element
# kit. The proposal is dropped and a typed forbidden_signature of the
# form ``kit_for_rca_violation:rca=<RCA>:lever=<LEVER>:singleton``
# is appended so the next iteration's LLM sees why it was rejected.
#
# Membership rules — a kit is admissible for an RCA in this map when
# it satisfies BOTH:
#   1. ``len(selected_levers) >= 2`` (no singletons), AND
#   2. ``set(selected_levers) & companions`` is non-empty — at least
#      one declared lever appears in the RCA's companion set.
#
# The companion sets below name CANONICAL lever-IDs (``lever-1`` ..
# ``lever-6``) drawn from the same closed enum as the Stage 3 LLM's
# ``selected_levers`` output.
#
# Coverage rationale (from the postmortem catalog):
#
#   * ``value_mapping_missing`` — a value-mapping instruction
#     (``lever-5a``) alone has been observed to leave the planner
#     guessing on the SQL shape. A snippet/example (``lever-5b`` or
#     ``lever-6``) provides the grammar anchor.
#   * ``join_semantics_wrong`` — join semantics are not reliably
#     fixable by prose alone; the planner needs an explicit join
#     pattern via ``lever-6`` (sql_snippet_expression) AND a
#     description (``lever-1``) explaining the join key.
#   * ``time_grain_wrong`` — time-grain corrections require an
#     example showing the correct grain (``lever-5b``) PLUS a
#     description / instruction reinforcing it (``lever-1`` or
#     ``lever-5a``).
#   * ``column_disambiguation`` — when two columns share a label,
#     the planner picks deterministically; a column description
#     (``lever-1``) MUST be paired with a snippet filter or
#     expression (``lever-6``) that exercises the disambiguation.
#   * ``table_routing_wrong`` — the planner routed to the wrong
#     fact / dimension table. A table-level description (``lever-2``)
#     pairs with an example_sql (``lever-5b``) showing the correct
#     route.
KIT_FOR_RCA: Mapping[str, frozenset[str]] = {
    "value_mapping_missing": frozenset({"lever-5b", "lever-6", "lever-5a"}),
    "join_semantics_wrong": frozenset({"lever-1", "lever-6"}),
    "time_grain_wrong": frozenset({"lever-1", "lever-5a", "lever-5b"}),
    "column_disambiguation": frozenset({"lever-1", "lever-6"}),
    "table_routing_wrong": frozenset({"lever-2", "lever-5b"}),
}


# ── Trial 24 — Kit at Source extension (flag-gated) ───────────────────
#
# The e943 / d139 postmortems showed example-SQL-insufficient RCA kinds
# (``extra_defensive_filter``, ``top_n_cardinality_collapse``) emitting a
# corrective LONE single lever (e.g. ``add_instruction``) that then died
# at the slate ``required_assets`` gate as ``unjustified_single_lever``
# before any Trial 23 repair hook could reach it. These RCAs were absent
# from ``KIT_FOR_RCA``, so the synthesizer was free to emit the lone
# lever. Trial 24 makes W4's ``RCA_KIND_TO_FIXING_MECHANISMS`` routing
# authoritative AS A KIT: the companion sets below are the lever-id
# projection of those fixing mechanisms (INSTRUCTION_TEXT -> lever-5a,
# SQL_SNIPPET -> lever-6, METADATA_DESCRIPTION -> lever-1), so the
# corrective patch is born as a >= 2-lever-family kit. Merged into the
# active companion lookup ONLY when ``trial24_kit_at_source_enabled()``
# is true; the base ``KIT_FOR_RCA`` constant is never mutated so flag-off
# is byte-stable.
_TRIAL24_KIT_FOR_RCA: Mapping[str, frozenset[str]] = {
    "extra_defensive_filter": frozenset({"lever-5a", "lever-6"}),
    "top_n_cardinality_collapse": frozenset({"lever-6", "lever-1"}),
}


# ── Trial 26 W26.2 — kit-map coverage expansion (flag-gated) ──────────
#
# Trial 24's kit gate proved correct in synthesis (`KIT_FORCED_V1` on
# the deterministic replay) but never fired on either canonical anchor
# in production because the live RCA distribution
# (``wrong_aggregation``, ``wrong_column``, ``plural_top_n_collapse``)
# fell outside the kit map. Trial 26 W26.2 widens coverage to the live
# distribution using the same lever-projection contract as Trial 24:
#   * ``wrong_aggregation``  → SQL_SNIPPET + METADATA_DESCRIPTION  →
#                              kit {lever-6, lever-1}
#   * ``wrong_column``       → METADATA_DESCRIPTION + INSTRUCTION_TEXT
#                              → kit {lever-1, lever-5a}
#   * ``plural_top_n_collapse`` → aliased upstream to
#     ``top_n_cardinality_collapse`` (kit {lever-6, lever-1}) by the
#     normaliser in ``rca_mechanism_routing``; no separate entry here.
#
# Merged into the active lookup ONLY when
# :func:`trial26_kit_map_expanded_enabled` is true; the Trial 24 map
# is never mutated so master-off restores Trial-24 behaviour byte-stably.
_TRIAL26_KIT_FOR_RCA: Mapping[str, frozenset[str]] = {
    "wrong_aggregation": frozenset({"lever-6", "lever-1"}),
    "wrong_column": frozenset({"lever-1", "lever-5a"}),
}


# Trial 26 W26.2 aliases applied at the action_groups normaliser layer.
# Kept in sync with ``rca_mechanism_routing._TRIAL26_RCA_KIND_ALIASES``
# so both layers reduce ``plural_top_n_collapse`` to the same canonical
# key when the sub-flag is ON. Sync is enforced by the alignment test
# (``test_trial26_kit_map_expansion.py``).
_TRIAL26_RCA_KIND_ALIASES: Mapping[str, str] = {
    "plural_top_n_collapse": "top_n_cardinality_collapse",
}


def _trial26_kit_map_expanded() -> bool:
    """Lazy + safe accessor for the Trial 26 W26.2 sub-flag.

    Returns False on any import error so a broken trial26_flags module
    cannot regress the Trial 24 baseline.
    """
    try:
        from genie_space_optimizer.optimization.trial26_flags import (
            trial26_kit_map_expanded_enabled,
        )
        return bool(trial26_kit_map_expanded_enabled())
    except Exception:
        return False


def _kit_for_rca_companions(key: str) -> frozenset[str] | None:
    """Return the companion lever set for an ``rca_kind`` key.

    The input is re-normalised internally so alias resolution
    (e.g. Trial 26 W26.2 ``plural_top_n_collapse`` →
    ``top_n_cardinality_collapse``) takes effect even when callers
    pass an unnormalised label. Idempotent on already-normalised
    inputs.

    Consults the base :data:`KIT_FOR_RCA` map, plus the Trial 24
    extension when :func:`trial24_kit_at_source_enabled` is on, and
    finally the Trial 26 W26.2 extension when
    :func:`trial26_kit_map_expanded_enabled` is on. Returns ``None``
    when the RCA has no kit contract (caller treats that as
    "single-lever allowed"). The base map always wins for keys it owns,
    so the Trial 24 / Trial 26 extensions can only ADD new RCA
    contracts, never weaken an existing one.

    On a Trial-26-expanded hit, emits one
    ``GSO_TRIAL26_KIT_MAP_EXPANDED_V1`` marker so postmortems can
    prove the new coverage is reachable. Legacy keys never emit it.
    """
    key = _normalize_rca_kind(key) if key else key
    base = KIT_FOR_RCA.get(key)
    if base is not None:
        return base
    try:
        from genie_space_optimizer.optimization.trial24_flags import (
            trial24_filter_removal_solo_enabled,
            trial24_kit_at_source_enabled,
        )

        if trial24_kit_at_source_enabled():
            # Follow-on B — ``extra_defensive_filter`` is a filter-REMOVAL
            # RCA whose fix is a lone instruction telling the planner not
            # to inject the predicate; a positive SQL-snippet companion
            # cannot express removal. When filter-removal-solo is on, drop
            # it from the forced-kit lookup so the P2.2 kit-violation gate
            # does not hard-reject the justified solo instruction.
            # ``top_n_cardinality_collapse`` stays a kit.
            if (
                key == "extra_defensive_filter"
                and trial24_filter_removal_solo_enabled()
            ):
                return None
            t24 = _TRIAL24_KIT_FOR_RCA.get(key)
            if t24 is not None:
                return t24
    except Exception:
        return None
    if _trial26_kit_map_expanded():
        t26 = _TRIAL26_KIT_FOR_RCA.get(key)
        if t26 is not None:
            _emit_trial26_kit_map_expanded_marker(
                rca_kind=key, companion_levers=t26
            )
            return t26
    return None


def _emit_trial26_kit_map_expanded_marker(
    *,
    rca_kind: str,
    companion_levers: frozenset[str],
) -> None:
    """Trial 26 W26.2 marker — record that the expanded kit map fired.

    Never raises — best-effort observability. Postmortems use the marker
    count to gate "is W26.2 reaching production?".
    """
    try:
        import json as _t26_json
        print(
            "GSO_TRIAL26_KIT_MAP_EXPANDED_V1 "
            + _t26_json.dumps(
                {
                    "rca_kind": rca_kind,
                    "companion_levers": sorted(companion_levers),
                },
                sort_keys=True,
                default=str,
            ),
            flush=True,
        )
    except Exception:
        pass


def active_kit_for_rca_map() -> Mapping[str, frozenset[str]]:
    """Return the full set of RCA kinds that currently carry a
    ``KIT_FOR_RCA`` companion contract, merged across the base map and
    every flag-gated extension that is ON.

    This is the single source of truth the Stage 3 synthesis prompt
    consults so its kit-mandate enumeration stays in lock-step with the
    validator (:func:`kit_for_rca_violation_reason`). Before Trial 26
    W26.2, the prompt hard-coded the two original Trial 24 kinds, so
    when the kit map was expanded the validator demanded a kit the
    producer was never told to emit — the desync that stranded
    ``wrong_aggregation`` / ``wrong_column`` proposals as
    ``kit_for_rca_violation:...:singleton``. Deriving the prompt from
    this accessor closes that gap generically: any future map expansion
    is reflected in the prompt with no further edit.

    Honours the SAME flag logic as :func:`_kit_for_rca_companions`:

      * the base :data:`KIT_FOR_RCA` always contributes;
      * :data:`_TRIAL24_KIT_FOR_RCA` is merged when
        :func:`trial24_kit_at_source_enabled` is on, EXCEPT
        ``extra_defensive_filter`` when
        :func:`trial24_filter_removal_solo_enabled` is on (that RCA is
        a justified solo instruction, not a kit);
      * :data:`_TRIAL26_KIT_FOR_RCA` is merged when
        :func:`trial26_kit_map_expanded_enabled` is on.

    The base map wins for keys it owns, so the extensions can only ADD
    contracts. Returns a plain dict (never mutates the module-level
    constants). Unlike :func:`_kit_for_rca_companions`, this accessor
    emits no markers — it is an enumeration helper, not a per-lookup
    gate.
    """
    merged: dict[str, frozenset[str]] = dict(KIT_FOR_RCA)
    try:
        from genie_space_optimizer.optimization.trial24_flags import (
            trial24_filter_removal_solo_enabled,
            trial24_kit_at_source_enabled,
        )

        if trial24_kit_at_source_enabled():
            _filter_solo = trial24_filter_removal_solo_enabled()
            for rca, companions in _TRIAL24_KIT_FOR_RCA.items():
                if rca in merged:
                    continue
                if rca == "extra_defensive_filter" and _filter_solo:
                    continue
                merged[rca] = companions
    except Exception:
        return merged
    if _trial26_kit_map_expanded():
        for rca, companions in _TRIAL26_KIT_FOR_RCA.items():
            merged.setdefault(rca, companions)
    return merged


def _trial26_rca_canonical_normalise() -> bool:
    """Lazy + safe accessor for the Trial 26 W26.1 sub-flag.

    Returns False on any import error so a broken trial26_flags module
    cannot regress the (pre-W26.1) baseline.
    """
    try:
        from genie_space_optimizer.optimization.trial26_flags import (
            trial26_rca_kind_canonical_normalise_enabled,
        )

        return trial26_rca_kind_canonical_normalise_enabled()
    except Exception:
        return False


def _normalize_rca_kind(rca_kind: str | None) -> str:
    """Phase 2 P2.2 — collapse free-text RCA labels to the closed
    KIT_FOR_RCA key vocabulary.

    The Stage 1 prompt emits ``rca_kind`` from a closed enum, but
    legacy diagnoses and replay fixtures sometimes carry leading /
    trailing whitespace or different casing. Normalize to lowercase
    and strip; unknown values pass through unchanged so the caller
    can use a simple ``in KIT_FOR_RCA`` membership test.

    Trial 26 W26.2 widens the alias table when the sub-flag is ON
    (``plural_top_n_collapse`` → ``top_n_cardinality_collapse``).

    Trial 26 W26.1 layers the canonical-key normaliser on top: when
    the sub-flag is ON, a free-form English label
    (``"Top-N cardinality collapse via spurious RANK()=1 filter"``)
    is collapsed onto its canonical key (``top_n_cardinality_collapse``)
    before the kit-map lookup so the Trial 24 / Trial 26 kit-at-source
    gate can finally fire on the live RCA distribution. Resolution
    falls back to the legacy normaliser only when the canonicaliser
    cannot find a canonical match (``unknown_kind``), preserving
    byte-stable behaviour for non-English keys.
    """
    if not rca_kind:
        return ""
    key = str(rca_kind).strip().lower()
    if _trial26_kit_map_expanded():
        key = _TRIAL26_RCA_KIND_ALIASES.get(key, key)
    if _trial26_rca_canonical_normalise():
        try:
            from genie_space_optimizer.optimization.rca_kind_canonical import (
                RCA_CANONICAL_KEY_SET,
                canonicalise_rca_kind,
            )

            result = canonicalise_rca_kind(rca_kind)
            if (
                result.canonical_key != "unknown_kind"
                and result.canonical_key in RCA_CANONICAL_KEY_SET
            ):
                return result.canonical_key
        except Exception:
            # Defense in depth — canonicaliser failures must never
            # regress the kit map. Fall through to the legacy key.
            pass
    return key


def kit_for_rca_violation_reason(
    rca_kind: str | None,
    selected_levers: Sequence[str],
) -> str:
    """Phase 2 P2.2 — return the typed forbidden_signature when a
    proposal violates the KIT_FOR_RCA contract, or ``""`` when the
    proposal is admissible.

    Returns one of the following deterministic shapes (consumed
    verbatim by the next iteration's ``forbidden_signatures``
    serializer):

      * ``""`` (empty) — proposal is admissible.
      * ``"kit_for_rca_violation:rca=<RCA>:singleton"`` —
        ``rca_kind`` is in the map and the kit has fewer than 2
        entries.
      * ``"kit_for_rca_violation:rca=<RCA>:no_companion"`` —
        ``rca_kind`` is in the map, the kit has >=2 entries, but
        none of them appear in the companion set.

    When ``rca_kind`` is NOT in ``KIT_FOR_RCA``, the function
    returns ``""``  — RCAs outside the map have no KIT contract and
    may freely emit single-lever proposals.
    """
    key = _normalize_rca_kind(rca_kind)
    companions = _kit_for_rca_companions(key)
    if companions is None:
        return ""
    kit = tuple(s for s in selected_levers if s)
    if len(kit) < 2:
        return f"kit_for_rca_violation:rca={key}:singleton"
    if not (set(kit) & companions):
        return f"kit_for_rca_violation:rca={key}:no_companion"
    return ""


def recommended_levers_for_cluster(cluster: dict) -> tuple[int, ...]:
    """Cycle 2 Task 4 — return the strategist's preferred lever
    ordering for a cluster.

    When ``GSO_QUESTION_SHAPE_LEVER_PREFERENCE`` is on and the cluster
    has ``q_count == 1`` AND ``root_cause`` is a question-shape root
    cause, returns the per-question lever set (3, 5) WITHOUT lever 6.
    Otherwise returns the default lever set that includes lever 6.
    """
    from genie_space_optimizer.common.config import (
        question_shape_lever_preference_enabled,
    )

    if not question_shape_lever_preference_enabled():
        return _DEFAULT_RECOMMENDED_LEVERS

    qids = cluster.get("question_ids") or []
    q_count = int(cluster.get("q_count") or len(qids) or 0)
    root_cause = str(cluster.get("root_cause") or "")
    if q_count == 1 and root_cause in _QUESTION_SHAPE_ROOT_CAUSES:
        return _PER_QUESTION_PREFERRED_LEVERS
    return _DEFAULT_RECOMMENDED_LEVERS


def stamp_recommended_levers_on_clusters(
    clusters: list[dict],
) -> list[dict]:
    """Cycle 2 Task 4 closeout — stamp ``recommended_levers`` on each
    cluster post-``rank_clusters`` so the strategist's ``ranking_text``
    builder can surface the per-cluster lever hint to the LLM.

    Returns a NEW list of NEW dicts (does not mutate input). Idempotent —
    re-stamping a cluster overwrites the prior ``recommended_levers``
    with the same value.
    """
    out: list[dict] = []
    for cluster in clusters:
        c = dict(cluster)
        c["recommended_levers"] = list(recommended_levers_for_cluster(c))
        out.append(c)
    return out


@dataclass
class ActionGroupsInput(JsonRoundTrip):
    """Input to stages.action_groups.select.

    ``action_groups`` is the slate of AGs the strategist returned (after
    filtering and buffered-AG drain — F4 doesn't re-do that work).
    ``source_clusters_by_id`` maps cluster id to cluster dict so each
    AG's root_cause can be recovered. ``rca_id_by_cluster`` maps cluster
    id to its RCA id. ``ag_alternatives_by_id`` carries Phase D.5
    rejected-alternatives stamping.

    C15 Phase 3: ``forbidden_ags`` carries the typed forbidden-AG set so
    select() can produce a per-candidate AdmissionTrace when
    ``stage_handlers_chunk_b_enabled()`` is on.

    Defect Plan 1 (2026-05-12): ``blocked_cluster_ids`` carries the set
    of cluster ids that the AG-emit prelude
    (``harness.collect_blocked_clusters``) marked as ungrounded. When
    non-empty AND ``ag_emit_grounding_gate_enabled()`` is True,
    ``select()`` drops every AG whose ``source_cluster_ids``
    intersects this set. Empty tuple preserves pre-defect-plan-1
    byte-stability.
    """

    action_groups: tuple[Mapping[str, Any], ...]
    source_clusters_by_id: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict
    )
    rca_id_by_cluster: Mapping[str, str] = field(default_factory=dict)
    ag_alternatives_by_id: Mapping[str, Sequence[AlternativeOption]] = field(
        default_factory=dict
    )
    # Optimizer Control-Plane Hardening Plan — Task C. Maps qid -> the
    # bucket the prior iteration assigned. When
    # ``GSO_BUCKET_DRIVEN_AG_SELECTION`` is on, ``select`` drops
    # MODEL_CEILING qids from AG target sets and tags AGs whose targets
    # are all EVIDENCE_GAP with ``ag_kind="evidence_gathering"``.
    prior_buckets_by_qid: Mapping[str, Any] = field(default_factory=dict)
    # Cycle 5 T2 — gate-drops carrying a causal-target patch from the
    # prior iteration. Empty unless the prior iteration captured drops.
    # Surfaced to the strategist's prompt context when
    # ``GSO_CAUSAL_DROP_FEEDBACK_TO_STRATEGIST`` is on so the LLM can
    # propose a narrower variant or shift levers instead of re-emitting
    # the same dropped pattern. Typed as ``tuple[Any, ...]`` to avoid a
    # circular import on ``stages.gates.DroppedCausalPatch``.
    prior_iteration_dropped_causal_patches: tuple[Any, ...] = ()
    # C15 Phase 3 — typed forbidden-AG set forwarded from
    # _compute_forbidden_ag_set. Empty unless stage_handlers_chunk_b_enabled().
    # When non-empty, select() records an AdmissionTrace per candidate.
    forbidden_ags: tuple[ForbiddenAG, ...] = ()
    # Defect Plan 1 (2026-05-12) — cluster ids with rca_card=False at
    # AG-emit time. select() drops AGs whose source_cluster_ids
    # intersect this set when ag_emit_grounding_gate_enabled().
    blocked_cluster_ids: tuple[str, ...] = ()
    # Phase 1.5 — ordered cluster priority list
    # (regressed + uncovered + original_target) built by
    # ``build_recovery_priority_list`` and threaded by the harness when
    # ``strategist_recovery_pivot_enabled()`` is on. Empty tuple is the
    # legacy / flag-off / iter-1 sentinel and is byte-stable: today
    # ``select()`` plumbs the field through unchanged for downstream
    # observability; honoring the order requires a follow-up commit.
    priority_cluster_ids: tuple[str, ...] = ()

    @classmethod
    def from_json(cls, payload: dict) -> "ActionGroupsInput":
        ags = tuple(
            dict(a) for a in (payload.get("action_groups") or [])
        )
        src = {
            str(k): dict(v)
            for k, v in (payload.get("source_clusters_by_id") or {}).items()
        }
        rca_by_cluster = {
            str(k): str(v)
            for k, v in (payload.get("rca_id_by_cluster") or {}).items()
        }
        ag_alts = {
            str(k): tuple(v)
            for k, v in (payload.get("ag_alternatives_by_id") or {}).items()
        }
        buckets = dict(payload.get("prior_buckets_by_qid") or {})
        dropped = tuple(payload.get("prior_iteration_dropped_causal_patches") or [])
        forbidden = tuple(
            ForbiddenAG.from_json(f)
            for f in (payload.get("forbidden_ags") or [])
        )
        blocked = tuple(
            str(c) for c in (payload.get("blocked_cluster_ids") or [])
        )
        priority = tuple(
            str(c) for c in (payload.get("priority_cluster_ids") or [])
        )
        return cls(
            action_groups=ags,
            source_clusters_by_id=src,
            rca_id_by_cluster=rca_by_cluster,
            ag_alternatives_by_id=ag_alts,
            prior_buckets_by_qid=buckets,
            prior_iteration_dropped_causal_patches=dropped,
            forbidden_ags=forbidden,
            blocked_cluster_ids=blocked,
            priority_cluster_ids=priority,
        )


@dataclass
class ActionGroupSlate(JsonRoundTrip):
    """Output of stages.action_groups.select.

    ``ags`` is the selected AG tuple (same content as input but
    normalized to a tuple). ``rejected_ag_alternatives`` records AGs
    the strategist proposed but the constraint/buffer pipeline
    filtered out, for Phase D.5 alternatives capture.

    C15 Phase 3: ``admission_trace`` records per-candidate
    AdmissionTrace entries when stage_handlers_chunk_b_enabled() is
    on. Empty tuple when flag is off (byte-stable with legacy
    behaviour — zero new fields emitted to postmortem bundle unless
    flag is on).

    Plan 1 Task 12: ``ag_records`` is the typed-ActionGroup sidecar.
    Derived from ``ags`` in __post_init__ via
    ``ActionGroup.from_legacy`` when not supplied. Malformed AGs (no
    ``id`` / ``ag_id``) are skipped silently from typed records;
    legacy ``ags`` tuple keeps them.
    """

    ags: tuple[Mapping[str, Any], ...]
    rejected_ag_alternatives: tuple[Mapping[str, Any], ...] = ()
    # C15 Phase 3 — per-candidate admission verdicts. Populated when
    # stage_handlers_chunk_b_enabled() is on; always empty otherwise.
    admission_trace: tuple[AdmissionTrace, ...] = ()
    # Plan 1 Task 12 — typed-ActionGroup sidecar.
    ag_records: tuple["ActionGroup", ...] = ()

    def __post_init__(self) -> None:
        if not self.ag_records and self.ags:
            from genie_space_optimizer.optimization.action_group import (
                ActionGroup,
            )
            derived: list[ActionGroup] = []
            for ag in self.ags:
                try:
                    derived.append(ActionGroup.from_legacy(ag))
                except ValueError:
                    continue
            self.ag_records = tuple(derived)

    @classmethod
    def from_json(cls, payload: dict) -> "ActionGroupSlate":
        ags = tuple(
            dict(a) for a in (payload.get("ags") or [])
        )
        rejected = tuple(
            dict(r) for r in (payload.get("rejected_ag_alternatives") or [])
        )
        trace = tuple(
            AdmissionTrace.from_json(t)
            for t in (payload.get("admission_trace") or [])
        )
        return cls(ags=ags, rejected_ag_alternatives=rejected, admission_trace=trace)


def _apply_bucket_policy(
    action_groups: tuple[Mapping[str, Any], ...],
    *,
    buckets_by_qid: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Optimizer Control-Plane Hardening Plan — Task C policy.

    Drop ``MODEL_CEILING`` qids from each AG's target set; if the AG
    ends up with no qids, drop the AG entirely. AGs whose remaining
    target qids are all ``EVIDENCE_GAP`` are tagged with
    ``ag_kind="evidence_gathering"`` so the proposal stage emits a
    no-op evidence-gathering proposal rather than mutating the space.
    """
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket,
    )

    out: list[dict[str, Any]] = []
    for ag in action_groups:
        target_qids = tuple(
            str(q) for q in (ag.get("target_qids") or ())
        )
        kept_qids = tuple(
            q for q in target_qids
            if buckets_by_qid.get(q) is not FailureBucket.MODEL_CEILING
        )
        if not kept_qids:
            continue
        new_ag = dict(ag)
        new_ag["target_qids"] = kept_qids
        affected = tuple(
            str(q) for q in (ag.get("affected_questions") or ())
        )
        if affected:
            new_ag["affected_questions"] = tuple(
                q for q in affected
                if buckets_by_qid.get(q) is not FailureBucket.MODEL_CEILING
            ) or kept_qids
        all_evidence_gap = all(
            buckets_by_qid.get(q) is FailureBucket.EVIDENCE_GAP
            for q in kept_qids
        )
        if all_evidence_gap:
            new_ag["ag_kind"] = "evidence_gathering"
        out.append(new_ag)
    return out


def normalize_strategist_ags_with_recommended_levers(
    *,
    ags,
    clusters,
):
    """Cycle 11 — union ``cluster.recommended_levers`` into every
    strategist-emit AG's ``lever_directives``. Mirrors the union the
    coverage path performs (control_plane.union_ag_levers_with_recommended)
    so the strategist path stops drifting from cluster RCA.

    Pure. No-op when ``GSO_AG_LEVERS_UNION_STRATEGIST_PATH=0`` or
    ``GSO_AG_LEVERS_UNION_RECOMMENDED=0``.
    """
    from genie_space_optimizer.common.config import (
        ag_levers_union_recommended_enabled,
        ag_levers_union_strategist_path_enabled,
    )
    if not (
        ag_levers_union_recommended_enabled()
        and ag_levers_union_strategist_path_enabled()
    ):
        return list(ags or [])

    from genie_space_optimizer.optimization.control_plane import (
        union_ag_levers_with_recommended,
    )

    cluster_by_id = {
        str(c.get("cluster_id") or ""): c for c in (clusters or [])
        if c.get("cluster_id")
    }
    out = []
    for ag in ags or []:
        src_ids = [
            str(cid) for cid in (ag.get("source_cluster_ids") or [])
            if str(cid)
        ]
        if not src_ids:
            out.append(ag)
            continue
        primary = cluster_by_id.get(src_ids[0]) or {}
        out.append(union_ag_levers_with_recommended(ag=ag, cluster=primary))
    return out


def _build_admission_trace(
    candidates: tuple[Mapping[str, Any], ...],
    forbidden_ags: tuple[ForbiddenAG, ...],
) -> tuple[AdmissionTrace, ...]:
    """C15 Phase 3 — produce per-candidate AdmissionTrace entries.

    Each candidate whose ``id``/``ag_id`` key matches a ``ForbiddenAG``
    gets verdict=DENIED with ``denial_reason`` set to the ForbiddenReason
    value. All other candidates get verdict=ADMITTED.

    Pure; no side effects. Called only when stage_handlers_chunk_b_enabled().
    """
    forbidden_by_id = {f.ag_id: f for f in forbidden_ags}
    trace: list[AdmissionTrace] = []
    for cand in candidates:
        ag_id = str(cand.get("id") or cand.get("ag_id") or "")
        if not ag_id:
            continue
        if ag_id in forbidden_by_id:
            trace.append(AdmissionTrace(
                ag_id=ag_id,
                verdict=AdmissionVerdict.DENIED,
                denial_reason=forbidden_by_id[ag_id].reason.value,
            ))
        else:
            trace.append(AdmissionTrace(ag_id=ag_id, verdict=AdmissionVerdict.ADMITTED))
    return tuple(trace)


def select(ctx, inp: ActionGroupsInput) -> ActionGroupSlate:
    """Stage 4 entry. Emits STRATEGIST_AG_EMITTED records and returns a
    typed slate. F4 is observability-only — does NOT invoke the
    strategist LLM, drain buffered AGs, or apply constraints. Harness
    still owns those steps and feeds the result into ``inp.action_groups``
    when the harness wire-up lands in a follow-up plan.

    Optimizer Control-Plane Hardening Plan — Task C. When
    ``GSO_BUCKET_DRIVEN_AG_SELECTION`` is on AND ``prior_buckets_by_qid``
    is non-empty, the slate is filtered through ``_apply_bucket_policy``
    before STRATEGIST_AG_EMITTED records are produced.

    C15 Phase 3: when stage_handlers_chunk_b_enabled() is on AND
    ``inp.forbidden_ags`` is non-empty, populates ``ActionGroupSlate.
    admission_trace`` with per-candidate verdicts so the postmortem bundle
    can surface which AGs were denied and why (forbidden-AG no-op loop
    observability). Flag-off behaviour is byte-stable with pre-Phase-3
    runs (admission_trace is always an empty tuple when flag is off).

    RCO-7 Site 2: ``inp.action_groups`` and ``inp.forbidden_ags`` are
    pre-sorted by canonical AG id before any downstream walk so the
    stage's outputs are independent of incidental LLM-output ordering.
    Harness-side sort (Site 1) is the first defense; this is
    defense-in-depth at the stage boundary.
    """
    from genie_space_optimizer.common.config import (
        ag_emit_grounding_gate_enabled,
        bucket_driven_ag_selection_enabled,
        stage_handlers_chunk_b_enabled,
    )
    from genie_space_optimizer.optimization.llm_boundary_sort import (
        sort_action_groups_canonically,
    )

    # RCO-7 Site 2 — canonical pre-sort.
    sorted_action_groups = tuple(
        sort_action_groups_canonically(inp.action_groups)
    )
    sorted_forbidden_ags = tuple(
        sorted(inp.forbidden_ags, key=lambda f: f.ag_id)
    )

    if (
        bucket_driven_ag_selection_enabled()
        and inp.prior_buckets_by_qid
    ):
        filtered_ags = tuple(
            _apply_bucket_policy(
                sorted_action_groups,
                buckets_by_qid=inp.prior_buckets_by_qid,
            )
        )
    else:
        filtered_ags = sorted_action_groups

    # Defect Plan 1 (2026-05-12) — grounding gate. When the flag is
    # on AND inp.blocked_cluster_ids is non-empty, drop every AG whose
    # source_cluster_ids intersect the blocked set. The harness has
    # already emitted one CLUSTER_BLOCKED_NO_RCA decision record per
    # blocked cluster, so the postmortem operator transcript shows why
    # the AG was dropped.
    if ag_emit_grounding_gate_enabled() and inp.blocked_cluster_ids:
        blocked = set(inp.blocked_cluster_ids)
        filtered_ags = tuple(
            ag for ag in filtered_ags
            if not (set(ag.get("source_cluster_ids") or []) & blocked)
        )

    # Cycle 11 Task 13 — union cluster.recommended_levers into
    # strategist-emit AG lever_directives so the strategist path
    # honours cluster RCA. Closes 7NOW H002 drift.
    filtered_ags = tuple(
        normalize_strategist_ags_with_recommended_levers(
            ags=list(filtered_ags),
            clusters=list(inp.source_clusters_by_id.values())
                if inp.source_clusters_by_id else [],
        )
    )

    records = strategist_ag_records(
        run_id=ctx.run_id,
        iteration=ctx.iteration,
        action_groups=filtered_ags,
        source_clusters_by_id=inp.source_clusters_by_id,
        rca_id_by_cluster=inp.rca_id_by_cluster,
        ag_alternatives_by_id=inp.ag_alternatives_by_id,
    )
    for record in records:
        ctx.decision_emit(record)

    # C15 Phase 3 — admission trace (chunk_b flag-gated; byte-stable when off).
    # RCO-7 Site 2 — feed the canonically sorted tuples so admission
    # trace order is deterministic.
    admission_trace: tuple[AdmissionTrace, ...] = ()
    if stage_handlers_chunk_b_enabled() and sorted_forbidden_ags:
        admission_trace = _build_admission_trace(
            candidates=sorted_action_groups,
            forbidden_ags=sorted_forbidden_ags,
        )

    return ActionGroupSlate(
        ags=filtered_ags,
        rejected_ag_alternatives=(),
        admission_trace=admission_trace,
    )


def materialize_diagnostic_ag(
    *,
    cluster: Mapping[str, Any],
    rca_id_by_cluster: Mapping[str, str],
) -> dict[str, Any]:
    """Optimizer Control-Plane Hardening Plan — Task F.

    Build a diagnostic AG for ``cluster`` that inherits its ``rca_id``.

    Used when the strategist did not emit an AG for a hard cluster in
    this iteration but the harness wants to attempt a diagnostic-only
    pass. The inherited ``rca_id`` propagates to every proposal at the
    F5 stage entry (Task D), keeping these proposals out of the
    ``rca_groundedness`` gate's drop set.
    """
    cluster_id = str(cluster.get("id") or "")
    rca_id = str(rca_id_by_cluster.get(cluster_id) or "")
    has_parent_rca = bool(rca_id)
    return {
        "id": f"AG_COVERAGE_{cluster_id}",
        "ag_id": f"AG_COVERAGE_{cluster_id}",
        # Cycle 5 T3 — split the diagnostic AG kind so the harness can
        # route the no-parent-RCA case to a regeneration step before
        # proposal generation. With parent RCA present (existing AG-1-F
        # path), the AG is ``"diagnostic"`` and proceeds normally;
        # without it, ``"diagnostic_no_parent_rca"`` signals to the
        # harness that ``ag_kind == "diagnostic_no_parent_rca"`` AND
        # ``needs_rca_regeneration is True`` together require an RCA
        # regen attempt before generating proposals.
        "ag_kind": "diagnostic" if has_parent_rca else "diagnostic_no_parent_rca",
        "needs_rca_regeneration": not has_parent_rca,
        "rca_id": rca_id,
        "primary_cluster_id": cluster_id,
        "source_cluster_ids": (cluster_id,),
        "target_qids": tuple(
            str(q) for q in (cluster.get("qids") or ())
        ),
        "affected_questions": tuple(
            str(q) for q in (cluster.get("qids") or ())
        ),
    }


# ── Phase H: explicit Input/Output class declarations ─────────────────
# Phase H's per-stage I/O capture decorator imports these to serialize
# the stage's typed input and output to MLflow.
INPUT_CLASS = ActionGroupsInput
OUTPUT_CLASS = ActionGroupSlate


# ── G-lite: uniform execute() alias ───────────────────────────────────
# The named verb above is preserved for human-readable harness call
# sites. The ``execute`` alias is what the stage registry, conformance
# test, and Phase H capture decorator import.
execute = select


# ── Plan 12 PR 5 — AG retry policy ─────────────────────────────────────


# Closed vocabulary of terminal reasons that demand a patch-family
# pivot on the NEXT iteration's AG for the same cluster. The strings
# match :class:`TerminalReason` enum values verbatim.
#
# ``narrow_loop_exhausted`` is a forward-looking entry: it's not in
# TerminalReason today, but Plan 12 PR 4's narrow-replacement loop
# will produce it once the harness wire-in lands. Keeping it in the
# set means the policy is already correct when that enum value is
# added; no membership check breaks in the meantime because
# build_terminal_signature() would refuse to construct one with that
# (non-existent) reason today.
_TERMINATIONS_REQUIRING_PIVOT: frozenset[str] = frozenset({
    "no_applied_patches",
    "structural_gate_dropped_instruction_only",
    "narrow_loop_exhausted",
    "applyability_rejected",
    # Trial 20 B3 — KEPT_INSUFFICIENT is a survival failure: patches
    # applied but produced behaviour-unchanged candidates. Plan 12
    # must treat this as a pivot trigger so the next iteration does
    # not retry the same lever family. See ``trial20_flags
    # .trial20_kept_insufficient_terminal_enabled``.
    "kept_insufficient",
})

# When a pivot is required, prefer ``add_example_sql`` — the most
# forgiving patch family (no SQL-validation surface, no structural
# repair gates, no blast-radius collision risk beyond the question
# itself). This is the canonical "we tried structural / instruction
# and it didn't apply; teach by example" fallback. Kept for the
# pre-Trial-20 byte-stable path when
# ``trial20_family_pivot_graph_enabled`` is OFF.
_PIVOT_FROM_FAMILY_AFTER_FAILURE: str = "add_example_sql"


# Trial 20 C1 — cycle-aware patch-family pivot graph. Replaces the
# degenerate single-element constant when the cluster's prior family
# is already ``add_example_sql`` (the postmortem 7now case: Plan 12
# returned ``add_example_sql`` again because the constant maps to
# itself, so ``pivot_recommended=false``).
#
# The cycle is closed under five families, so any prior family has a
# distinct next destination. Order is intentional: structural ->
# example -> filter -> expression -> metadata -> structural. This
# is illustrative, not the LLM's plan — it's a fallback when the
# strategist asks "what family should I avoid retrying" after a
# survival failure.
_PIVOT_GRAPH: dict[str, str] = {
    "add_instruction": "add_example_sql",
    "add_example_sql": "add_sql_snippet_filter",
    "add_sql_snippet_filter": "add_sql_snippet_expression",
    "add_sql_snippet_expression": "add_column_description",
    "add_column_description": "add_instruction",
}


def _infer_prior_family_from_signatures(
    prior_terminal_signatures: "list",
) -> str:
    """Trial 20 C2 — when ``prior_patch_family`` is empty, infer it
    from the most recent kept-insufficient / applier-record signature
    on the cluster instead of defaulting to ``add_example_sql``.

    Phase 2 P2.5 — the acceptance gate now stamps the patch family
    directly onto :class:`TerminalSignature.prior_patch_family`, so
    we consult that authoritative field FIRST. We fall back to the
    legacy attribute scan (``patch_family`` / ``patch_type`` /
    ``insufficient_repair_signature``) only for pre-P2.5 signatures
    that did not stamp the new field.

    The inference walks signatures in reverse chronological order
    and returns the first non-empty hit. Returns the empty string
    when no signature carries the field — caller falls back to the
    legacy default.
    """
    for sig in (prior_terminal_signatures or ())[::-1]:
        # Phase 2 P2.5 — authoritative field on the new
        # TerminalSignature dataclass.
        ppf = getattr(sig, "prior_patch_family", "")
        if isinstance(ppf, str) and ppf.strip():
            return ppf.strip()
        for attr in ("patch_family", "patch_type"):
            val = getattr(sig, attr, None)
            if isinstance(val, str) and val.strip():
                return val.strip()
        irs = getattr(sig, "insufficient_repair_signature", None)
        if irs is not None:
            for attr in ("patch_family", "patch_type"):
                val = getattr(irs, attr, None)
                if isinstance(val, str) and val.strip():
                    return val.strip()
    return ""


# ── Phase 2 P2.5 — lever_id → patch_family map ──────────────────────
#
# Mirrors the canonical ``LEVER_TO_PATCH_TYPES`` definition in
# ``levers_contract.py`` but inverts the relationship to pick a
# REPRESENTATIVE patch_family per lever_id. The pivot helper below
# uses this to translate a chosen companion lever back to the
# patch_family vocabulary that ``_PIVOT_GRAPH`` keys on.
#
# Choice of representative is deterministic: the family that
# postmortems most often observe as the "primary" execution shape
# for the lever. For ``lever-5`` (which spans add_instruction +
# add_example_sql_*) we choose ``add_example_sql`` because the
# example-shape is what the planner actually anchors on — the prose
# instructions alone are a known weak pivot.
_LEVER_ID_TO_PRIMARY_FAMILY: dict[str, str] = {
    "lever-1": "add_column_description",
    "lever-2": "add_description",
    "lever-3": "add_example_sql",
    "lever-4": "add_join_spec",
    "lever-5": "add_example_sql",
    "lever-5a": "add_instruction",
    "lever-5b": "add_example_sql",
    "lever-6": "add_sql_snippet_filter",
}


def next_companion_family_from_kit(
    rca_kind: str | None,
    prior_terminal_signatures: "list",
) -> str:
    """Phase 2 P2.5 — pick the next patch_family for the cluster by
    consulting :data:`KIT_FOR_RCA`.

    When ``rca_kind`` is in the KIT_FOR_RCA companion map, we already
    know the canonical kit for that diagnosis. The prior signatures
    tell us which lever_ids the LLM has already exhausted (via the
    new ``prior_lever_set`` field on each :class:`TerminalSignature`).
    The pivot policy is:

      1. Compute the companion set ``KIT_FOR_RCA[rca_kind]``.
      2. Compute the already-tried lever_id set as the union of
         ``prior_lever_set`` over all prior signatures.
      3. Pick the FIRST companion lever (in deterministic
         ``sorted()`` order) NOT in the tried set.
      4. Translate that lever to its representative patch_family
         via :data:`_LEVER_ID_TO_PRIMARY_FAMILY`.

    Returns the empty string when:
      * ``rca_kind`` is NOT in KIT_FOR_RCA (no companion contract
        to consult — caller falls back to the legacy
        ``_PIVOT_GRAPH`` policy);
      * every companion lever has already been tried (the cluster
        has exhausted its KIT_FOR_RCA companions — caller terminates
        with FALLBACK_NO_NEW_STRATEGY);
      * the chosen lever has no representative family mapping
        (defensive — should never fire for the closed lever_id
        enum, but keeps the helper total).

    This function replaces the cyclic ``_PIVOT_GRAPH`` step for
    diagnoses that have a typed kit contract; legacy diagnoses
    outside KIT_FOR_RCA continue to use ``_PIVOT_GRAPH``.
    """
    key = _normalize_rca_kind(rca_kind)
    companions = _kit_for_rca_companions(key)
    if companions is None:
        return ""
    tried: set[str] = set()
    for sig in (prior_terminal_signatures or ())[::-1]:
        prior_lever_set = getattr(sig, "prior_lever_set", None)
        if prior_lever_set:
            tried.update(str(s) for s in prior_lever_set if s)
    for lever_id in sorted(companions):
        if lever_id in tried:
            continue
        family = _LEVER_ID_TO_PRIMARY_FAMILY.get(lever_id, "")
        if family:
            return family
    return ""


def regenerate_action_groups_with_signatures(
    *,
    prior_clusters: list,
    prior_terminal_signatures: "list",
    existing_forbidden_set: set,
    inner_regenerate,
    insufficient_repair_signatures: "list | tuple | None" = None,
    **kwargs,
):
    """Plan 12 — wrapper that adds prior :class:`TerminalSignature`
    entries to the forbidden_set BEFORE delegating to the real AG
    regenerator. Closes the ``ag_collision_with_forbidden_set``
    retry-budget waste both 2026-05-20 postmortems observed: the
    regenerator was called with a stale forbidden_set, the LLM
    proposed the same AG again, and the iteration burned budget
    without producing a meaningful retry.

    ``existing_forbidden_set`` is preserved by union; the wrapper
    never replaces a caller-provided set.

    The wrapper itself does no LLM work — it just makes the
    forbidden_set complete before `inner_regenerate` runs.

    Trial 19 A3 — when ``insufficient_repair_signatures`` is supplied
    (the Trial 18 sibling channel from ``KEPT_INSUFFICIENT``), each
    signature is unioned into the forbidden_set so the regenerator
    cannot re-emit an AG that maps to a previously-insufficient
    repair shape. The wrapper also passes the raw sequence through
    to ``inner_regenerate`` as ``insufficient_repair_signatures=`` so
    the regenerator's Stage 2 prompt can render the typed feedback
    on the new AG it produces. Optional; pre-Trial-19 callers that
    omit the kwarg get byte-stable behavior.
    """
    expanded = set(existing_forbidden_set or set())
    for sig in (prior_terminal_signatures or ()):
        expanded.add(sig)
    insufficient_tuple = tuple(insufficient_repair_signatures or ())
    for sig in insufficient_tuple:
        if sig:
            expanded.add(sig)
    if insufficient_tuple:
        kwargs.setdefault(
            "insufficient_repair_signatures", insufficient_tuple,
        )
    regenerated = inner_regenerate(
        prior_clusters=prior_clusters,
        forbidden_set=expanded,
        **kwargs,
    )

    # Trial 19 A6 — fallback_no_new_strategy detection. If the inner
    # regenerator returned an empty / falsy result AND we had at
    # least one prior terminal or insufficient signature in the
    # expanded set, the iteration has exhausted strategies and the
    # caller should consume the typed terminal reason instead of
    # treating the empty return as ``no_action_group_emitted`` (which
    # the strategist path also emits, but for a different cause:
    # zero AGs from a fresh LLM call vs. zero AGs from the fallback
    # collision path).
    try:
        from genie_space_optimizer.optimization.trial19_flags import (
            trial19_enforce_insufficient_enabled,
        )
        if (
            trial19_enforce_insufficient_enabled()
            and not regenerated
            and (prior_terminal_signatures or insufficient_tuple)
        ):
            import json as _json
            # Phase 3 P3.2 — centralized marker payload helper. The
            # schema is now pinned by ``fallback_marker_payload`` so
            # postmortems can grep one symbol rather than reading the
            # JSON literal here. Downstream callers in the harness
            # classify the iteration's terminal_reason via
            # ``classify_zero_ag_terminal_reason`` (same module) — when
            # this marker fires, the classifier MUST return
            # ``FALLBACK_NO_NEW_STRATEGY`` rather than the catch-all
            # ``NO_ACTION_GROUP_EMITTED``.
            from genie_space_optimizer.optimization.fallback_terminal import (
                fallback_marker_payload as _fallback_marker_payload,
            )
            print(
                "GSO_FALLBACK_NO_NEW_STRATEGY_V1 "
                + _json.dumps(
                    _fallback_marker_payload(
                        expanded_forbidden_count=len(expanded),
                        insufficient_repair_signatures_count=len(
                            insufficient_tuple
                        ),
                        prior_terminal_signatures_count=len(
                            list(prior_terminal_signatures or ())
                        ),
                    ),
                    sort_keys=True,
                    default=str,
                ),
                flush=True,
            )
    except Exception:
        # Defensive — marker emission must not break legacy callers.
        pass

    return regenerated


def next_patch_family_for_cluster(
    *,
    cluster_id: str,
    prior_terminal_signatures: "list",
    prior_patch_family: str,
) -> str:
    """Plan 12 — choose the next patch family for a cluster.

    If the most recent terminal signature carries a survival-failure
    ``terminal_reason`` in :data:`_TERMINATIONS_REQUIRING_PIVOT`,
    pivot to the next family per :data:`_PIVOT_GRAPH` (Trial 20 C1).
    Otherwise, retain the prior family.

    Trial 20 C1 — when ``trial20_family_pivot_graph_enabled`` is ON,
    the pivot target is ``_PIVOT_GRAPH[prior_patch_family]`` so the
    degenerate "pivot from add_example_sql back to add_example_sql"
    case (postmortem 7now) becomes "pivot to add_sql_snippet_filter".
    Trial 20 C2 — when ``prior_patch_family`` is empty/unknown, infer
    it from the most recent kept-insufficient or applier-record
    signature instead of defaulting to ``add_example_sql``.

    When the Trial 20 flag is OFF, the function preserves the
    pre-Trial-20 byte-stable behaviour (constant pivot target).

    ``cluster_id`` is accepted for future per-cluster policy
    (currently the policy is global) and to make the signature
    readable at call sites.
    """
    del cluster_id  # Reserved for per-cluster policy refinement.
    from genie_space_optimizer.optimization.trial20_flags import (
        trial20_family_pivot_graph_enabled,
    )

    pivot_required = False
    for sig in (prior_terminal_signatures or ())[::-1]:
        reason = str(getattr(sig, "terminal_reason", "") or "")
        if reason in _TERMINATIONS_REQUIRING_PIVOT:
            pivot_required = True
            break

    if not pivot_required:
        return str(prior_patch_family or _PIVOT_FROM_FAMILY_AFTER_FAILURE)

    if not trial20_family_pivot_graph_enabled():
        return _PIVOT_FROM_FAMILY_AFTER_FAILURE

    # Phase 2 P2.5 — prefer the KIT_FOR_RCA companion pivot when the
    # most recent signature names an RCA in the map. The companion
    # picker walks the canonical kit and returns the first
    # un-tried companion lever's representative patch_family. This
    # replaces the cyclic ``_PIVOT_GRAPH`` next-family heuristic for
    # diagnoses where the kit contract gives us a typed answer.
    most_recent_rca = ""
    for sig in (prior_terminal_signatures or ())[::-1]:
        rca = getattr(sig, "root_cause", "")
        if isinstance(rca, str) and rca.strip():
            most_recent_rca = rca.strip()
            break
    companion_family = next_companion_family_from_kit(
        most_recent_rca, prior_terminal_signatures
    )
    if companion_family:
        return companion_family

    effective_prior_family = str(prior_patch_family or "").strip()
    if not effective_prior_family:
        effective_prior_family = _infer_prior_family_from_signatures(
            prior_terminal_signatures
        )
    if not effective_prior_family:
        return _PIVOT_FROM_FAMILY_AFTER_FAILURE
    return _PIVOT_GRAPH.get(
        effective_prior_family, _PIVOT_FROM_FAMILY_AFTER_FAILURE
    )
