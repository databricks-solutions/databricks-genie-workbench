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
