"""Phase 1 (2026-05-17) — FailureCluster typed contract.

Carries all five identifier aliases for a Genie Space failure
across the critical synthesis path:

- cluster_id (e.g. "H001") — the bucket label, stable across
  iterations of the same AG.
- target_qids (e.g. ("..._gs_013",)) — the canonical question
  identifiers. The retired-signature producer
  (harness._compute_forbidden_ag_set_pair) keys on these.
- affected_questions — an alias of target_qids exposed as a
  derived property because some legacy paths read this name.
- root_cause / asi_failure_type / failure_keys — the failure
  taxonomy from the ASI.
- blame_set_raw / blame_set_normalized — ASI blame tokens and
  their post-normalization form. blame_set_normalized empty
  means the resolver failed (the Layer A root cause observed
  in both live runs).
- rca_card_id / rca_card_summary / is_grounded — RCA grounding
  state.

Projections (``to_nsc_marker_payload``, ``to_decision_record_kwargs``,
``collision_key_pair``) refuse construction when causal fields the
upstream stage knew are empty. This is the architectural
invariant that makes silent ``""`` in postmortems impossible.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from genie_space_optimizer.optimization.repair_intent import RepairShape


class FailureClusterIdentityError(ValueError):
    """Raised when cluster and AG dicts disagree on identity fields.

    The two live runs that motivated this plan never produced
    inconsistent identity dicts in production — but the unit tests
    that masked Phase 0.1's bug DID, by accident. Make it loud.
    """


@dataclass(frozen=True)
class FailureCluster:
    """A typed view of a Genie Space failure cluster."""

    cluster_id: str
    target_qids: tuple[str, ...]
    root_cause: str
    asi_failure_type: str
    failure_keys: tuple[str, ...]
    blame_set_raw: tuple[str, ...]
    blame_set_normalized: tuple[str, ...]
    rca_card_id: str
    rca_card_summary: str
    is_grounded: bool
    # ── Plan 4 — typed semantic fields populated by the LLM path. ──
    # Defaults preserve byte-stable construction for every existing
    # caller of FailureCluster(...) and from_legacy(...).
    semantic_theme: str = ""
    suggested_repair_shape: RepairShape = RepairShape.OTHER

    @property
    def affected_questions(self) -> tuple[str, ...]:
        """Derived alias used by legacy code that reads
        ``affected_questions`` instead of ``target_qids``."""
        return self.target_qids

    @classmethod
    def from_legacy(
        cls, cluster: Mapping, ag: Mapping | None = None
    ) -> "FailureCluster":
        """Build a FailureCluster from the legacy cluster + ag dicts.

        ``ag`` may be None (we are working from a cluster alone, e.g.
        in cluster-driven safety-net dispatch). When both are present,
        target_qids from cluster.question_ids must agree with
        ag.affected_questions; otherwise FailureClusterIdentityError.
        """
        cluster_id = str(cluster.get("cluster_id") or "")
        cluster_qids = tuple(
            str(q) for q in (cluster.get("question_ids") or []) if str(q)
        )
        if ag is not None:
            ag_qids = tuple(
                str(q) for q in (ag.get("affected_questions") or []) if str(q)
            )
            if cluster_qids and ag_qids and set(cluster_qids) != set(ag_qids):
                raise FailureClusterIdentityError(
                    f"Cluster/AG identity mismatch on target_qids for "
                    f"cluster_id={cluster_id!r}: cluster.question_ids="
                    f"{sorted(cluster_qids)} vs ag.affected_questions="
                    f"{sorted(ag_qids)}. The retired-signature producer "
                    f"keys on these qids; an inconsistent pair makes "
                    f"the typed-contract guarantee impossible to honour."
                )
            target_qids = ag_qids or cluster_qids
        else:
            target_qids = cluster_qids

        rca_card = cluster.get("rca_card")
        if isinstance(rca_card, Mapping) and rca_card:
            rca_card_id = str(
                rca_card.get("id") or rca_card.get("rca_id") or ""
            )
            rca_card_summary = str(rca_card.get("root_cause_summary") or "")
            is_grounded = True
        else:
            rca_card_id = ""
            rca_card_summary = ""
            is_grounded = False

        failure_keys_list = list(
            cluster.get("failure_keys")
            or [
                k
                for k in (
                    cluster.get("root_cause"),
                    cluster.get("asi_failure_type"),
                )
                if k
            ]
        )
        failure_keys = tuple(str(k) for k in failure_keys_list if k)

        blame_raw = tuple(
            str(b) for b in (cluster.get("asi_blame_set") or []) if str(b)
        )
        blame_normalized = tuple(
            str(b)
            for b in (cluster.get("asi_blame_set_normalized") or [])
            if str(b)
        )

        return cls(
            cluster_id=cluster_id,
            target_qids=target_qids,
            root_cause=str(cluster.get("root_cause") or ""),
            asi_failure_type=str(cluster.get("asi_failure_type") or ""),
            failure_keys=failure_keys,
            blame_set_raw=blame_raw,
            blame_set_normalized=blame_normalized,
            rca_card_id=rca_card_id,
            rca_card_summary=rca_card_summary,
            is_grounded=is_grounded,
        )

    def collision_key_pair(self, *, lever_keys: list[int]) -> Any:
        """Build the canonical collision key for retired-AG
        admission. Uses target_qids consistently — typed
        replacement for the Phase 0.1 fix in
        harness._ag_collision_key_pair."""
        from genie_space_optimizer.optimization.harness import (
            _CollisionKeyPair,
        )

        lever_frozen = (
            frozenset(int(lk) for lk in lever_keys)
            if lever_keys
            else frozenset()
        )
        if self.target_qids and lever_keys:
            terminal_signature_keys = (
                (
                    frozenset(self.target_qids),
                    lever_frozen,
                ),
            )
        else:
            terminal_signature_keys = ()
        return _CollisionKeyPair(
            root_cause_key=None,
            signature_keys=(),
            terminal_signature_keys=terminal_signature_keys,
        )

    def to_nsc_marker_payload(
        self,
        *,
        ag_id: str,
        iteration: int,
        skipped_reason: str,
        attempted_archetypes: tuple[str, ...],
    ) -> dict[str, Any]:
        """Project to the GSO_NO_STRUCTURAL_CANDIDATE_V1 marker
        payload.

        REFUSES when both skipped_reason and attempted_archetypes
        are empty. The synthesizer always knows something:

        - If it ran and produced no candidate, it sets a typed
          skipped_reason (one of the 9 declines in
          cluster_driven_synthesis.py).
        - If it ran and produced candidates that all failed gates,
          it populates attempted_archetypes.
        - If it never ran (e.g. RCA-card pre-flight refused), the
          dispatcher sets skipped_reason="missing_rca_card".

        Both empty therefore indicates a code path where causal
        context was dropped; raise so CI catches it.
        """
        if not skipped_reason and not attempted_archetypes:
            raise ValueError(
                f"to_nsc_marker_payload: synthesizer must report "
                f"either a non-empty skipped_reason or a non-empty "
                f"attempted_archetypes tuple. Got both empty for "
                f"cluster_id={self.cluster_id!r}, "
                f"target_qids={sorted(self.target_qids)}. This is "
                f"the Phase 1 refuse-on-empty invariant; a None "
                f"reason indicates upstream context loss."
            )
        return {
            "ag_id": ag_id,
            "iteration": int(iteration),
            "attempted_archetypes": list(attempted_archetypes),
            "skipped_reason": str(skipped_reason or ""),
        }

    def to_decision_record_kwargs(
        self,
        *,
        run_id: str,
        iteration: int,
        ag_id: str,
        skipped_reason: str,
        attempted_archetypes: tuple[str, ...],
    ) -> dict[str, Any]:
        """Project to the kwargs dict for
        ``no_structural_candidate_record``.

        Same refuse-on-empty invariant as ``to_nsc_marker_payload``.
        """
        if not skipped_reason and not attempted_archetypes:
            raise ValueError(
                f"to_decision_record_kwargs: synthesizer must report "
                f"either a non-empty skipped_reason or a non-empty "
                f"attempted_archetypes tuple. Got both empty for "
                f"cluster_id={self.cluster_id!r}."
            )
        return dict(
            run_id=run_id,
            iteration=int(iteration),
            ag_id=str(ag_id),
            cluster_id=self.cluster_id,
            rca_id=self.rca_card_id,
            root_cause=self.root_cause,
            target_qids=self.target_qids,
            attempted_archetypes=attempted_archetypes,
            skipped_reason=str(skipped_reason or ""),
        )

    @classmethod
    def from_llm_cluster(
        cls,
        llm_cluster: Any,  # LlmCluster; Any avoids cross-module cycles.
        ag: Mapping | None = None,
    ) -> "FailureCluster":
        """Build a FailureCluster from an LlmCluster.

        ``ag`` is optional; when present, ``target_qids`` from the
        LlmCluster must agree with ``ag.affected_questions`` (mirrors
        the identity-check contract of ``from_legacy``). On mismatch
        raises ``FailureClusterIdentityError``.
        """
        llm_qids = tuple(
            str(q) for q in llm_cluster.member_qids if str(q)
        )
        if ag is not None:
            ag_qids = tuple(
                str(q) for q in (ag.get("affected_questions") or [])
                if str(q)
            )
            if llm_qids and ag_qids and set(llm_qids) != set(ag_qids):
                raise FailureClusterIdentityError(
                    f"LlmCluster/AG identity mismatch on target_qids "
                    f"for cluster_id={llm_cluster.cluster_id!r}: "
                    f"llm.member_qids={sorted(llm_qids)} vs "
                    f"ag.affected_questions={sorted(ag_qids)}."
                )

        return cls(
            cluster_id=str(llm_cluster.cluster_id),
            target_qids=llm_qids,
            root_cause=str(llm_cluster.semantic_theme),
            asi_failure_type=str(llm_cluster.suggested_repair_shape.value),
            failure_keys=(
                str(llm_cluster.semantic_theme),
                str(llm_cluster.suggested_repair_shape.value),
            ),
            blame_set_raw=tuple(
                str(b) for b in llm_cluster.primary_blame_set
            ),
            blame_set_normalized=tuple(
                str(b) for b in llm_cluster.primary_blame_set
            ),
            rca_card_id="",
            rca_card_summary="",
            is_grounded=False,
            semantic_theme=str(llm_cluster.semantic_theme),
            suggested_repair_shape=RepairShape(
                llm_cluster.suggested_repair_shape
            ),
        )
