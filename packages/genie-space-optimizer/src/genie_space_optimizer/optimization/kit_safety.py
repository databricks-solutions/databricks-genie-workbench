"""Phase 2 Action 2.2 — Kit-level safety summary + gate.

The kit-level gate runs *before* the per-patch blast-radius gate. It
operates on ``RepairKit`` objects (see ``optimization.repair_kit``) and
produces a ``KitSafetySummary`` artifact carrying:

* ``union_target_objects`` — every catalog object the kit modifies.
* ``union_passing_dependents`` — superset of qids passing on any kit
  target object.
* ``required_companions`` — tuple of patch types that must apply
  together for the kit to be causally complete (e.g. column description
  + synonym + negative-guidance instruction).
* ``expected_causal_effect`` — echoed from the kit so postmortems can
  attribute the gate's decision.
* ``target_qids`` — hard-cluster qids the kit claims to fix
  (echoed from ``kit.target_qids``). Used by ``kit_level_gate``'s
  correctness predicate ("does the kit's expected effect overlap any
  cluster target qid?").
* ``co_beneficiary_qids`` — soft-cluster qids that share root-cause
  evidence with the target cluster (matching counterfactual fix,
  matching wrong-clause pattern, or shared blame term within the same
  root-cause family). Phase 2 ships this as an explicit input to
  ``build_kit_safety_summary`` (parameter ``soft_evidence_matched_qids``,
  default ``()``); the matcher that populates it is **Phase 3 Action
  3.3**. Soft signals are RCA evidence and risk-reducing context only —
  never independent repair targets. The strategist input remains
  structurally hard-clusters-only.
* ``risk_class`` ∈ ``{low, medium, high}``. Computed pure-from-patches
  in ``build_kit_safety_summary`` — does NOT account for co-beneficiaries
  or scoped variants. The gate's ``effective_risk_class`` applies both
  downgrades (scoped alternative available, and co-beneficiary count
  ≥ ``co_beneficiary_downgrade_threshold``) so the summary stays a
  pure data artifact and the gate concentrates policy.
* ``scoped_alternative_available`` — True when at least one kit member
  carries a ``_scoped_variant_pid`` (Section C wires this).

The summary functions are pure (no I/O, no logger). The
``soft_evidence_matched_qids`` input flows from the wrapper
(``select_kit_aware_patch_cap``) which gets it from the harness, which
will get it from the Phase 3 Action 3.3 matcher when wired.
"""

from __future__ import annotations

from dataclasses import dataclass

from genie_space_optimizer.optimization.repair_kit import RepairKit


_NON_SEMANTIC_PATCH_TYPES: frozenset[str] = frozenset({
    "update_column_description",
    "add_column_synonym",
    "update_table_description",
    "add_metric_view_instruction",
    "add_table_instruction",
})

_INSTRUCTION_PATCH_TYPES: frozenset[str] = frozenset({
    "add_instruction",
    "update_instruction_section",
    "rewrite_instruction",
})

_SQL_SNIPPET_PATCH_TYPES: frozenset[str] = frozenset({
    "add_sql_snippet_filter",
    "add_sql_snippet_metric",
    "add_sql_snippet_dimension",
    "add_example_sql",
})

_HUB_TABLE_DEPENDENT_THRESHOLD: int = 5  # default; threshold is also surfaced as config


@dataclass(frozen=True)
class KitSafetySummary:
    kit_id: str
    repair_archetype: str
    union_target_objects: tuple[str, ...]
    union_passing_dependents: tuple[str, ...]
    required_companions: tuple[str, ...]
    expected_causal_effect: str
    target_qids: tuple[str, ...]  # hard-cluster qids the kit claims to fix
    risk_class: str  # low | medium | high — pure-from-patches; downgrades live in the gate
    co_beneficiary_qids: tuple[str, ...] = ()  # soft-cluster qids with shared evidence (Phase 3 populates)
    scoped_alternative_available: bool = False


def _patch_type(patch: dict) -> str:
    return str(patch.get("patch_type") or patch.get("type") or "")


def _patch_target_objects(patch: dict) -> list[str]:
    out: list[str] = []
    table = str(patch.get("target_table") or "").strip()
    column = str(patch.get("column") or "").strip()
    if table:
        out.append(table)
        if column:
            out.append(f"{table}.{column}")
    obj = str(patch.get("target_object") or "").strip()
    if obj:
        out.append(obj)
    return out


def _patch_passing_dependents(patch: dict) -> list[str]:
    raw = patch.get("passing_dependents")
    if raw is None:
        return []
    return [str(q) for q in raw if str(q)]


def _kit_classify_risk(kit: RepairKit) -> str:
    """Return ``low | medium | high`` for the kit.

    * ``low`` — only metadata edits (column descriptions, synonyms,
      table descriptions, table/MV instructions).
    * ``medium`` — instruction edits that are not flagged
      ``high_collateral_risk``.
    * ``high`` — any patch with ``high_collateral_risk = True``,
      OR any SQL-snippet patch with > 5 passing dependents,
      OR any patch whose union_passing_dependents (across the kit)
      individually crosses the hub-table threshold.
    """
    types = {_patch_type(p) for p in kit.patches}

    for p in kit.patches:
        if p.get("high_collateral_risk"):
            return "high"

    for p in kit.patches:
        if (
            _patch_type(p) in _SQL_SNIPPET_PATCH_TYPES
            and len(_patch_passing_dependents(p)) > _HUB_TABLE_DEPENDENT_THRESHOLD
        ):
            return "high"

    if types and types <= _NON_SEMANTIC_PATCH_TYPES:
        return "low"

    if types & _INSTRUCTION_PATCH_TYPES:
        return "medium"

    return "medium"


def _required_companions(kit: RepairKit) -> tuple[str, ...]:
    """Heuristic: dimension_disambiguation kits require both a
    column description and a synonym. Other archetypes' companions
    are encoded by the planner in the kit's required_companions
    field on the planner side; this function surfaces patch-type
    completeness for dimension_disambiguation as a baseline."""
    if kit.repair_archetype == "dimension_disambiguation":
        return ("update_column_description", "add_column_synonym")
    return ()


def _scoped_alternative_available(kit: RepairKit) -> bool:
    return any(p.get("_scoped_variant_pid") for p in kit.patches)


def build_kit_safety_summary(
    kit: RepairKit,
    *,
    soft_evidence_matched_qids: tuple[str, ...] = (),
) -> KitSafetySummary:
    """Pure function: assemble a KitSafetySummary for the kit.

    ``soft_evidence_matched_qids`` is the tuple of soft-cluster qids
    that share root-cause evidence with the kit's target hard cluster.
    Phase 2 ships this as ``()`` by default — the matcher that
    populates it lives in **Phase 3 Action 3.3**. The tuple is stored
    on ``KitSafetySummary.co_beneficiary_qids`` so the gate can apply
    the co-beneficiary downgrade in ``effective_risk_class``. The
    summary's own ``risk_class`` is computed pure-from-patches (no
    co-beneficiary or scoped-variant adjustment) so the data artifact
    stays policy-free; both downgrades live in ``kit_level_gate``.

    The hard-cluster ``target_qids`` are echoed from ``kit.target_qids``
    onto the summary so postmortems can attribute the kit-level gate's
    decision to a specific target list without re-reading the kit.
    """
    union_targets: set[str] = set()
    union_dependents: set[str] = set()
    for p in kit.patches:
        union_targets.update(_patch_target_objects(p))
        union_dependents.update(_patch_passing_dependents(p))
    target_set = {str(q) for q in kit.target_qids if str(q)}
    co_set = {str(q) for q in soft_evidence_matched_qids if str(q)} - target_set
    return KitSafetySummary(
        kit_id=kit.kit_id,
        repair_archetype=kit.repair_archetype,
        union_target_objects=tuple(sorted(union_targets)),
        union_passing_dependents=tuple(sorted(union_dependents)),
        required_companions=_required_companions(kit),
        expected_causal_effect=kit.expected_causal_effect,
        target_qids=tuple(kit.target_qids),
        risk_class=_kit_classify_risk(kit),
        co_beneficiary_qids=tuple(sorted(co_set)),
        scoped_alternative_available=_scoped_alternative_available(kit),
    )


# ---------------------------------------------------------------------------
# Phase 2 Action 2.2 — Kit-level gate.
# ---------------------------------------------------------------------------


_RISK_TIERS = ("low", "medium", "high")


def _downgrade_risk(risk: str) -> str:
    """Drop one tier (high → medium → low; low stays low)."""
    if risk not in _RISK_TIERS:
        return risk
    idx = _RISK_TIERS.index(risk)
    return _RISK_TIERS[max(0, idx - 1)]


@dataclass(frozen=True)
class KitSafetyPolicy:
    passing_dependents_threshold: int = 15
    co_beneficiary_downgrade_threshold: int = 5  # Phase 2 Action 2.2 default; tunable via GSO_KIT_CO_BENEFICIARY_DOWNGRADE_THRESHOLD


@dataclass(frozen=True)
class KitGateDecision:
    accepted: bool
    reason: str
    effective_risk_class: str  # may differ from summary.risk_class when scoped alt is available OR co-beneficiaries clear the threshold
    co_beneficiary_count: int = 0  # informational — populated for postmortem aggregation


def kit_level_gate(
    *,
    kit: RepairKit,
    summary: KitSafetySummary,
    policy: KitSafetyPolicy,
    cluster_target_qids: tuple[str, ...],
) -> KitGateDecision:
    """Pure function: decide whether the kit clears the kit-level gate.

    Reject when ANY of:
    1. ``kit.target_qids`` shares no element with ``cluster_target_qids``
       (the kit's expected effect doesn't touch any cluster qid).
    2. ``summary.union_passing_dependents`` count > ``policy.passing_dependents_threshold``.
    3. ``effective_risk_class == 'high'`` after both downgrades are applied.

    On acceptance, ``effective_risk_class`` reflects two independent
    downgrades, each applied at most once:

    * **Co-beneficiary downgrade** — when
      ``len(summary.co_beneficiary_qids) >= policy.co_beneficiary_downgrade_threshold``
      (default 5), drop one tier. Rationale: a patch with broad
      shared-evidence support is structurally safer than a 1-qid
      patch — it's more likely to fix root cause than to over-fit a
      single failure. Co-beneficiaries are evidence (Phase 3 Action 3.3
      populates them); this is risk-reducing context, NOT a target.
    * **Scoped-variant downgrade** — when ``summary.scoped_alternative_available``
      is ``True``, drop one tier. Section C wires this.

    Both downgrades floor at ``low``. The co-beneficiary downgrade is
    applied first so the scoped-variant downgrade can compose on top
    when both apply.
    """
    target_set = {str(q) for q in kit.target_qids if str(q)}
    cluster_set = {str(q) for q in cluster_target_qids if str(q)}
    if cluster_set and not (target_set & cluster_set):
        return KitGateDecision(
            accepted=False,
            reason="expected_effect_misses_target_qids",
            effective_risk_class=summary.risk_class,
            co_beneficiary_count=len(summary.co_beneficiary_qids),
        )

    if len(summary.union_passing_dependents) > policy.passing_dependents_threshold:
        return KitGateDecision(
            accepted=False,
            reason="union_passing_dependents_exceeds_threshold",
            effective_risk_class=summary.risk_class,
            co_beneficiary_count=len(summary.co_beneficiary_qids),
        )

    effective = summary.risk_class
    co_count = len(summary.co_beneficiary_qids)
    if co_count >= policy.co_beneficiary_downgrade_threshold:
        effective = _downgrade_risk(effective)
    if summary.scoped_alternative_available:
        effective = _downgrade_risk(effective)
    if effective == "high":
        return KitGateDecision(
            accepted=False,
            reason="high_risk_no_scoped_alternative",
            effective_risk_class=summary.risk_class,
            co_beneficiary_count=co_count,
        )

    return KitGateDecision(
        accepted=True,
        reason="kit_safe",
        effective_risk_class=effective,
        co_beneficiary_count=co_count,
    )


# ---------------------------------------------------------------------------
# Phase 2 Action 2.2 — Kit-aware patch-cap wrapper.
# ---------------------------------------------------------------------------


def select_kit_aware_patch_cap(
    patches: list[dict],
    *,
    target_qids: tuple[str, ...],
    max_patches: int,
    cluster_target_qids: tuple[str, ...],
    policy: KitSafetyPolicy,
    active_cluster_ids: tuple[str, ...] = (),
    per_cluster_slot_floor: int = 0,
    soft_evidence_matched_qids_by_kit: dict[str, tuple[str, ...]] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Phase 2 Action 2.2 — kit-aware wrapper around
    ``select_target_aware_causal_patch_cap``.

    Returns ``(selected_patches, legacy_decisions, kit_outcomes)``:

    * ``selected_patches`` — patches that survived BOTH the legacy
      target-aware cap AND the kit-level gate. Atomicity guarantee:
      every patch in the result belongs to a kit whose every other
      member is also in the result.
    * ``legacy_decisions`` — pass-through of the legacy cap's per-patch
      decision rows (kept / dropped reasons), unchanged.
    * ``kit_outcomes`` — one dict per kit with kit_id, accepted, reason,
      kept_count, total_count, co_beneficiary_count, effective_risk_class.

    ``soft_evidence_matched_qids_by_kit`` (Phase 3 Action 3.3 contract)
    maps each kit's ``kit_id`` to a tuple of soft-cluster qids that
    share root-cause evidence with the kit's hard target. The wrapper
    passes these to ``build_kit_safety_summary`` per-kit. Default
    ``None`` → all kits get ``()`` co-beneficiaries → the downgrade
    is a no-op (Phase 2 default).
    """
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )
    from genie_space_optimizer.optimization.repair_kit import (
        group_patches_into_kits,
    )

    if not patches:
        return [], [], []

    soft_lookup: dict[str, tuple[str, ...]] = soft_evidence_matched_qids_by_kit or {}

    kits = group_patches_into_kits(patches)
    legacy_selected, legacy_decisions = select_target_aware_causal_patch_cap(
        list(patches),
        target_qids=target_qids,
        max_patches=max_patches,
        active_cluster_ids=active_cluster_ids,
        per_cluster_slot_floor=per_cluster_slot_floor,
    )
    legacy_pid_set = {
        str(p.get("proposal_id") or p.get("id") or "") for p in legacy_selected
    }

    kit_outcomes: list[dict] = []
    selected: list[dict] = []

    for kit in kits:
        kit_pids = {
            str(p.get("proposal_id") or p.get("id") or "") for p in kit.patches
        }
        kept = kit_pids & legacy_pid_set
        kept_count = len(kept)
        total_count = len(kit_pids)
        soft_qids = soft_lookup.get(kit.kit_id, ())

        if kept_count == 0:
            kit_outcomes.append({
                "kit_id": kit.kit_id,
                "accepted": False,
                "reason": "kit_dropped_by_legacy_cap",
                "kept_count": 0,
                "total_count": total_count,
                "co_beneficiary_count": len(soft_qids),
                "effective_risk_class": "n/a",
            })
            continue

        if kept_count != total_count:
            kit_outcomes.append({
                "kit_id": kit.kit_id,
                "accepted": False,
                "reason": "kit_atomicity_violation",
                "kept_count": kept_count,
                "total_count": total_count,
                "co_beneficiary_count": len(soft_qids),
                "effective_risk_class": "n/a",
            })
            continue

        summary = build_kit_safety_summary(
            kit,
            soft_evidence_matched_qids=soft_qids,
        )
        decision = kit_level_gate(
            kit=kit,
            summary=summary,
            policy=policy,
            cluster_target_qids=cluster_target_qids,
        )
        if not decision.accepted:
            kit_outcomes.append({
                "kit_id": kit.kit_id,
                "accepted": False,
                "reason": decision.reason,
                "kept_count": 0,
                "total_count": total_count,
                "co_beneficiary_count": decision.co_beneficiary_count,
                "effective_risk_class": decision.effective_risk_class,
            })
            continue

        # Phase 2 Action 2.3 — surface the scoped-variant risk downgrade
        # so the harness can emit kit_risk_downgraded_by_scoped_variant.
        # Downgrade fires when the kit's pure-from-patches risk_class was
        # 'high' but the gate's effective_risk_class is one tier lower
        # because a scoped alternative was available.
        risk_downgraded = (
            summary.risk_class == "high"
            and summary.scoped_alternative_available
            and decision.effective_risk_class != "high"
        )
        kit_outcomes.append({
            "kit_id": kit.kit_id,
            "accepted": True,
            "reason": "kit_safe",
            "kept_count": total_count,
            "total_count": total_count,
            "co_beneficiary_count": decision.co_beneficiary_count,
            "effective_risk_class": decision.effective_risk_class,
            "risk_downgraded_from_high_to_medium": risk_downgraded,
        })
        selected.extend(kit.patches)

    return selected, legacy_decisions, kit_outcomes


# ---------------------------------------------------------------------------
# Phase 1 Addendum × Phase 2 Section B bridge.
# ---------------------------------------------------------------------------


def build_soft_evidence_lookup_by_kit(
    *,
    clusters: list[dict],
    patches: list[dict],
) -> dict[str, tuple[str, ...]]:
    """Phase 1 Addendum × Phase 2 Section B bridge.

    Joins each kit (computed deterministically from ``patches`` via
    ``group_patches_into_kits``) against the cluster object's
    ``rca_card_supporting_soft_evidence`` field (populated by Phase 1
    Addendum's ``build_rca_card`` when ``GSO_RCA_CARD_SOFT_EVIDENCE=1``).
    Returns ``{kit_id: tuple[str, ...]}`` ready to feed into
    ``select_kit_aware_patch_cap``'s ``soft_evidence_matched_qids_by_kit``
    parameter — the key set is identical to what
    ``group_patches_into_kits`` would compute downstream, so the
    wrapper's ``soft_lookup.get(kit.kit_id, ())`` succeeds when there
    is matched evidence.

    Pure function. Match strategy: a kit is paired with the first
    cluster whose ``question_ids`` contains the kit's first target qid.
    A kit with no target_qids OR no matching cluster OR an empty
    ``rca_card_supporting_soft_evidence`` list is omitted from the
    returned dict — passing the resulting dict directly to the wrapper
    means absent kits get ``()`` co-beneficiaries (default behaviour).
    """
    from genie_space_optimizer.optimization.repair_kit import (
        group_patches_into_kits,
    )

    # Index clusters by every qid they own so a kit's first target qid
    # finds its owning cluster in one lookup.
    cluster_by_qid: dict[str, dict] = {}
    for cluster in clusters or ():
        for qid in (cluster.get("question_ids") or ()):
            cluster_by_qid[str(qid)] = cluster

    out: dict[str, tuple[str, ...]] = {}
    for kit in group_patches_into_kits(patches or ()):
        if not kit.target_qids:
            continue
        owner = cluster_by_qid.get(str(kit.target_qids[0]))
        if owner is None:
            continue
        soft_entries = owner.get("rca_card_supporting_soft_evidence") or ()
        if not soft_entries:
            continue
        soft_qids = tuple(
            str(entry.get("soft_qid"))
            for entry in soft_entries
            if isinstance(entry, dict) and entry.get("soft_qid")
        )
        if soft_qids:
            out[kit.kit_id] = soft_qids
    return out
