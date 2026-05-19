"""Plan 8 Task 9 — harness-side input assembly for Plan 7.

Plan 7's ``hypothesize_next_attempts_for_iteration`` consumes five
typed inputs the harness assembles per iteration:

  * ``repair_intents_by_id`` — built from the legacy proposal list
    (``all_proposals``) via stamps from ``stamp_repair_intent_on_proposal``
  * ``cluster_id_by_intent_id`` — derived from the intents map
  * ``per_qid_evidence_by_cluster`` — re-groups RcaEvidenceBundle.
    per_qid_evidence_typed by metadata_snapshot["cluster_by_qid"]
  * ``applied_patch_fingerprints_by_ag`` — from the harness apply_log
  * ``identifier_allowlist_by_ag`` — per-AG identifier set
  * ``critique_verdicts_by_intent_id`` — from CritiqueOutcome via
    critique_verdict_index.verdict_by_intent_id_from_proposals_by_ag

The builders take the legacy harness shapes (``list[dict]`` per AG,
``dict[str, Any]`` for apply_log) — there is no typed ProposalSlate
in the harness's per-AG loop (see Plan 8 v2 scope banner #3).

This module is pure (no LLM, no MLflow, no global state) so the
unit tests can exercise each builder in isolation.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Mapping

from genie_space_optimizer.optimization.critique_verdict_index import (
    verdict_by_intent_id_from_proposals_by_ag,
)
from genie_space_optimizer.optimization.repair_intent import (
    extract_repair_intent_from_proposal,
)

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.candidate_critique_typed import (
        CritiqueVerdict,
    )
    from genie_space_optimizer.optimization.rca_evidence_typed import (
        PerQidRcaEvidence,
    )
    from genie_space_optimizer.optimization.repair_intent import RepairIntent
    from genie_space_optimizer.optimization.stages.candidate_critique import (
        CritiqueOutcome,
    )


def build_repair_intents_by_id_from_proposals(
    proposals: Iterable[Mapping[str, Any]],
) -> dict[str, "RepairIntent"]:
    """Build ``{intent_id: RepairIntent}`` from stamped proposal dicts.

    Plan 1's ``stamp_repair_intent_on_proposal`` writes ``intent_id``
    and ``repair_intent`` (the serialized RepairIntent JSON) onto each
    proposal dict. ``extract_repair_intent_from_proposal`` is the
    typed inverse. This builder runs the inverse over every proposal,
    dropping unstamped ones silently (Plan 8 Task 7 fixes the stamp
    gap at the source so the drop becomes a no-op in production).
    """
    out: dict[str, "RepairIntent"] = {}
    for p in (proposals or ()):
        try:
            intent = extract_repair_intent_from_proposal(p)
        except Exception:
            intent = None
        if intent is not None:
            out[intent.intent_id] = intent
    return out


def build_cluster_id_by_intent_id(
    repair_intents_by_id: Mapping[str, "RepairIntent"],
) -> dict[str, str]:
    """Return ``{intent_id: cluster_id}`` from the typed intents map."""
    return {
        str(intent_id): str(intent.cluster_id)
        for intent_id, intent in (repair_intents_by_id or {}).items()
        if intent.cluster_id
    }


def build_per_qid_evidence_by_cluster(
    per_qid_evidence_typed: dict[str, "PerQidRcaEvidence"],
    cluster_by_qid: dict[str, str],
) -> dict[str, dict[str, "PerQidRcaEvidence"]]:
    """Re-group typed RCA evidence by cluster_id."""
    out: dict[str, dict[str, Any]] = {}
    for qid, ev in (per_qid_evidence_typed or {}).items():
        cid = str((cluster_by_qid or {}).get(qid) or "")
        if not cid:
            continue
        out.setdefault(cid, {})[qid] = ev
    return out


def build_applied_patch_fingerprints_by_ag(
    apply_log: dict[str, Any],
) -> dict[str, set[str]]:
    """Build ``{ag_id: {fingerprint, ...}}`` from the apply_log."""
    out: dict[str, set[str]] = {}
    for entry in ((apply_log or {}).get("applied") or ()):
        patch = entry.get("patch") or {}
        ag_id = str(patch.get("ag_id") or "")
        fp = patch.get("content_fingerprint") or ""
        if not ag_id or not fp:
            continue
        if isinstance(fp, (list, tuple, set, frozenset)):
            for x in fp:
                if str(x):
                    out.setdefault(ag_id, set()).add(str(x))
        else:
            for x in str(fp).split(";"):
                x = x.strip()
                if x:
                    out.setdefault(ag_id, set()).add(x)
    return out


def build_identifier_allowlist_by_ag(
    *,
    ags: tuple[dict, ...],
    metadata_snapshot: dict[str, Any],
    cluster_by_qid: dict[str, str],
    per_qid_evidence_typed: dict[str, "PerQidRcaEvidence"],
) -> dict[str, set[str]]:
    """Build per-AG identifier allowlists.

    Falls back to the union of blame_set fields from the AG's
    cluster's typed evidence when ``metadata_snapshot["schema_columns"]``
    is empty (mirrors the inline derivation in
    ``_dispatch_lever_5b_for_cluster``).
    """
    schema_columns: set[str] = set(
        metadata_snapshot.get("schema_columns") or ()
    )
    out: dict[str, set[str]] = {}
    for ag in (ags or ()):
        ag_id = str(ag.get("id") or ag.get("ag_id") or "")
        if not ag_id:
            continue
        if schema_columns:
            out[ag_id] = set(schema_columns)
            continue
        ag_allow: set[str] = set()
        for qid in (ag.get("affected_questions") or ()):
            ev = (per_qid_evidence_typed or {}).get(str(qid))
            if ev is not None:
                ag_allow.update(str(b) for b in ev.blame_set)
        out[ag_id] = ag_allow
    return out


def build_critique_verdicts_by_intent_id(
    critique_outcome: "CritiqueOutcome | None",
    proposals_by_ag: Mapping[str, Iterable[Mapping[str, Any]]],
) -> dict[str, "CritiqueVerdict"]:
    """Pass-through to the Task 5 legacy-shape join helper.

    Accepts ``proposals_by_ag`` as a legacy ``{ag_id: list[dict]}``
    mapping (NOT a typed ProposalSlate — the harness has only the
    legacy list at the wire-in site; see Plan 8 v2 scope banner #3).
    Returns ``{}`` when ``critique_outcome`` is None (Plan 6 stage
    not run or all proposals dropped before critique).
    """
    if critique_outcome is None:
        return {}
    return verdict_by_intent_id_from_proposals_by_ag(
        critique_outcome, proposals_by_ag,
    )
