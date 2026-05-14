"""Plan 3 — typed input bundle for Stage-2 per-skill executors.

The Stage-1 discovery prompt picks ``applicable_skills`` from the Plan 1
catalogue. For each pick, ``build_activation_bundle`` constructs an
``ActivationBundle`` containing exactly what that skill's executor needs
— no more, no less. The bundle is frozen and hashable so it can be
used as a memoization key.

Plan 4 will populate the ``raw_evidence`` field (currently always
empty) for skills that pass the leakage classifier. Plan 3 leaves
``raw_evidence=()`` unconditionally.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


_VALID_PRIORITIES: frozenset[int] = frozenset({1, 2, 3})


@dataclass(frozen=True, eq=False)
class ActivationBundle:
    """Frozen typed input for a Stage-2 executor.

    Equality / hash use ``(skill_id, ag_id, target_objects)`` only —
    so two bundles for the same skill on the same targets compare
    equal even if their evidence text differs. This makes the bundle
    safe to use as a memoization key in
    ``three_stage_pipeline._stage_2_for_skill``.
    """
    skill_id: str
    ag_id: str
    target_objects: tuple[str, ...]
    cluster_afs: tuple[dict, ...]
    metadata_snapshot: dict
    identifier_allowlist: str
    evidence_refs: tuple[str, ...]
    expected_impact_qids: tuple[str, ...]
    raw_evidence: tuple[dict, ...]
    lever_directives_legacy: dict | None
    discovery_rationale: str
    priority: int

    def __post_init__(self) -> None:
        if not isinstance(self.target_objects, tuple):
            raise TypeError(
                f"target_objects must be tuple, got {type(self.target_objects).__name__}"
            )
        if not isinstance(self.cluster_afs, tuple):
            raise TypeError(
                f"cluster_afs must be tuple, got {type(self.cluster_afs).__name__}"
            )
        if not isinstance(self.evidence_refs, tuple):
            raise TypeError(
                f"evidence_refs must be tuple, got {type(self.evidence_refs).__name__}"
            )
        if not isinstance(self.expected_impact_qids, tuple):
            raise TypeError(
                f"expected_impact_qids must be tuple, got "
                f"{type(self.expected_impact_qids).__name__}"
            )
        if not isinstance(self.raw_evidence, tuple):
            raise TypeError(
                f"raw_evidence must be tuple, got {type(self.raw_evidence).__name__}"
            )
        if self.priority not in _VALID_PRIORITIES:
            raise ValueError(
                f"priority must be one of {sorted(_VALID_PRIORITIES)}, "
                f"got {self.priority!r}"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ActivationBundle):
            return NotImplemented
        return (
            self.skill_id == other.skill_id
            and self.ag_id == other.ag_id
            and self.target_objects == other.target_objects
        )

    def __hash__(self) -> int:
        return hash((self.skill_id, self.ag_id, self.target_objects))


def merge_skill_picks(picks: list[dict]) -> list[dict]:
    """Collapse Stage-1 picks that share a ``skill_id`` into one merged
    pick per skill_id.

    Merge rules:
      * ``target_objects`` — union (de-duplicated, sorted for determinism).
      * ``evidence_refs`` — union.
      * ``expected_impact_qids`` — union.
      * ``why`` — concatenated with ``" | "`` separator.
      * ``priority`` — minimum (highest urgency wins).

    Returns picks in their first-occurrence order. Rejects unknown
    skill_ids by passing them through unchanged — the dispatcher will
    log + skip.
    """
    by_id: dict[str, dict] = {}
    order: list[str] = []
    for pick in picks:
        sid = pick.get("skill_id", "")
        if not sid:
            continue
        if sid not in by_id:
            by_id[sid] = {
                "skill_id": sid,
                "target_objects": list(dict.fromkeys(pick.get("target_objects") or [])),
                "evidence_refs": list(dict.fromkeys(pick.get("evidence_refs") or [])),
                "expected_impact_qids": list(dict.fromkeys(
                    pick.get("expected_impact_qids") or []
                )),
                "why": str(pick.get("why", "")),
                "priority": int(pick.get("priority", 3) or 3),
            }
            order.append(sid)
            continue
        merged = by_id[sid]
        merged["target_objects"] = sorted(set(merged["target_objects"])
                                           | set(pick.get("target_objects") or []))
        merged["evidence_refs"] = sorted(set(merged["evidence_refs"])
                                          | set(pick.get("evidence_refs") or []))
        merged["expected_impact_qids"] = sorted(set(merged["expected_impact_qids"])
                                                 | set(pick.get("expected_impact_qids") or []))
        why_new = str(pick.get("why", ""))
        if why_new:
            merged["why"] = (merged["why"] + " | " + why_new).strip(" |")
        merged["priority"] = min(merged["priority"], int(pick.get("priority", 3) or 3))
    return [by_id[sid] for sid in order]


def build_activation_bundle(
    pick: dict,
    ag_id: str,
    clusters: list[dict],
    metadata_snapshot: dict,
    w: Any = None,
) -> ActivationBundle:
    """Translate one Stage-1 ``skill_pick`` + AG context → a typed
    ``ActivationBundle`` ready for Stage-2 dispatch.

    Plan 4: when ``raw_evidence_v1_enabled()`` is True,
    ``ActivationBundle.raw_evidence`` is populated with up to
    ``raw_evidence_n()`` (default 3) diverse triples per
    ``optimization.raw_evidence.project_evidence_for_skill``. When
    the flag is False, behavior is byte-stable with Plan 3
    (``raw_evidence=()``).

    The optional ``w`` kwarg is forwarded to the diverse-sampling
    layer so embedding-based selection can run when the workspace
    client is reachable. ``None`` falls back to deterministic
    n-gram-Jaccard sampling.
    """
    from genie_space_optimizer.optimization.afs import format_afs
    from genie_space_optimizer.optimization.optimizer import (
        _build_identifier_allowlist, _format_identifier_allowlist,
    )

    cluster_afs = tuple(format_afs(c) for c in (clusters or []))
    allowlist_struct = _build_identifier_allowlist(metadata_snapshot)
    allowlist_text = _format_identifier_allowlist(allowlist_struct)

    target_objects = tuple(sorted(set(pick.get("target_objects") or [])))
    evidence_refs = tuple(pick.get("evidence_refs") or [])
    expected_impact_qids = tuple(pick.get("expected_impact_qids") or [])
    priority = int(pick.get("priority", 3) or 3)
    if priority not in _VALID_PRIORITIES:
        priority = 3

    raw_evidence: tuple[dict, ...] = ()
    skill_id = str(pick.get("skill_id", ""))
    from genie_space_optimizer.common.config import (
        raw_evidence_n, raw_evidence_v1_enabled,
        _record_raw_evidence_projection,
    )
    if raw_evidence_v1_enabled():
        from genie_space_optimizer.optimization.raw_evidence import (
            project_evidence_for_skill,
        )
        raw_evidence = project_evidence_for_skill(
            skill_id=skill_id,
            clusters=clusters or [],
            w=w,
            n=raw_evidence_n(),
        )
        if raw_evidence:
            # Capture-sink hit ONLY when projection actually returned
            # evidence — empty projections don't count toward the
            # coverage gate (lever-5b returns empty by design; an
            # empty projection from a projectable skill means the
            # cluster had no failed-judge questions, which we don't
            # want to count either).
            _record_raw_evidence_projection(skill_id)

    return ActivationBundle(
        skill_id=skill_id,
        ag_id=str(ag_id),
        target_objects=target_objects,
        cluster_afs=cluster_afs,
        metadata_snapshot=metadata_snapshot,
        identifier_allowlist=allowlist_text,
        evidence_refs=evidence_refs,
        expected_impact_qids=expected_impact_qids,
        raw_evidence=raw_evidence,
        lever_directives_legacy=None,
        discovery_rationale=str(pick.get("why", "")),
        priority=priority,
    )
