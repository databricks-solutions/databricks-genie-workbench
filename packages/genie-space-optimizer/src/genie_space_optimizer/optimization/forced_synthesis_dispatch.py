"""Lever-5 forced-structural-synthesis dispatch — extracted from harness.py.

This module exists so the L5 forced-synthesis dispatch (formerly inline
at harness.py:22720-22929) is callable in isolation. The replay harness
calls this function with a stubbed ``synthesize`` callable to verify
dispatch behavior offline against frozen fixtures, without spinning up
the full optimizer.

The function preserves the EXACT behavior of the inline block — including
the label-divergence bug where ``_LEVER5_GATE_DROPS[*].root_causes`` (which
prefers ``asi_failure_type``) is compared with strict equality against
``cluster.root_cause`` (which is the RcaKind label). Fixing that bug is
Plan A's job, not this refactor's.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class ForcedSynthesisDispatchResult:
    """Per-call result of ``dispatch_forced_structural_synthesis``.

    Fields:
        attempted_dispatches: tuple of ``(cluster_id, root_cause)`` pairs
            the dispatch loop actually visited (i.e., where
            ``_should_force_structural_synthesis`` returned True AND a
            matching cluster was found in ``iter_source_clusters_by_id``).
            Empty when the label-divergence bug short-circuits the loop.
        appended_proposals: tuple of forced ``add_example_sql`` proposal
            dicts produced by successful synthesis. The harness appends
            these to ``all_proposals`` at the call site.
        emitted_decision_records: tuple of ``DecisionRecord.to_dict()``
            outputs for ``NO_STRUCTURAL_CANDIDATE`` cases. The harness
            extends ``_current_iter_inputs["decision_records"]`` with
            these at the call site.

    Exceptions are NOT caught inside the dispatch function — the harness
    call site's existing outer try-except handles them (same shape as
    the original inline block). This preserves byte-stable exception
    accounting via ``_phase_b_producer_exceptions``.
    """
    attempted_dispatches: tuple[tuple[str, str], ...]
    appended_proposals: tuple[dict[str, Any], ...]
    emitted_decision_records: tuple[dict[str, Any], ...]


def dispatch_forced_structural_synthesis(
    *,
    run_id: str,
    iteration: int,
    ag: Mapping[str, Any],
    l5_ag_drops: Sequence[Mapping[str, Any]],
    iter_source_clusters_by_id: Mapping[str, Mapping[str, Any]],
    iter_rca_id_by_cluster: Mapping[str, str],
    metadata_snapshot: Mapping[str, Any],
    benchmarks: Sequence[Mapping[str, Any]],
    catalog: str,
    schema: str,
    w: Any,
    spark: Any,
    lever_keys: Iterable[int],
    reflection_buffer: Sequence[Any],
    current_iter_inputs: dict[str, Any],
    synthesize: Callable[..., Any] | None = None,
) -> ForcedSynthesisDispatchResult:
    """Run the L5 forced-structural-synthesis dispatch for one AG.

    Parameters mirror the closure-of-locals pinned in Task 0. The
    ``synthesize`` parameter defaults to
    ``run_cluster_driven_synthesis_for_single_cluster`` from
    ``cluster_driven_synthesis`` (resolved lazily inside the function
    to avoid circular imports); tests pass a stub.

    Returns a ``ForcedSynthesisDispatchResult`` instead of mutating the
    caller's locals directly. The harness call site applies the side
    effects (append to ``all_proposals``, extend
    ``_current_iter_inputs["decision_records"]``, bump
    ``_phase_b_producer_exceptions``).

    BUG PRESERVED — the strict-equality cluster lookup at the inner
    ``for _cid in _drop.get("source_clusters")`` loop matches
    ``cluster.root_cause`` against ``_drop.root_causes[*]``. The latter
    prefers ``asi_failure_type`` (per optimizer.py:15338-15342), so
    SQL-shape clusters whose ``asi_failure_type`` differs from their
    RcaKind ``root_cause`` are silently skipped. Plan A fixes this; this
    refactor only moves the bug into a place where it is testable.
    """
    import logging
    logger = logging.getLogger(__name__)

    attempted: list[tuple[str, str]] = []
    appended: list[dict[str, Any]] = []
    emitted: list[dict[str, Any]] = []

    if not l5_ag_drops:
        return ForcedSynthesisDispatchResult(
            attempted_dispatches=(),
            appended_proposals=(),
            emitted_decision_records=(),
        )

    # Resolve the synthesize callable lazily to avoid circular imports.
    if synthesize is None:
        from genie_space_optimizer.optimization.cluster_driven_synthesis import (
            run_cluster_driven_synthesis_for_single_cluster as _default_synth,
        )
        synthesize = _default_synth

    from genie_space_optimizer.optimization.decision_emitters import (
        no_structural_candidate_record,
    )
    from genie_space_optimizer.optimization.harness import (
        _should_force_structural_synthesis,
    )
    from genie_space_optimizer.common.warehouse import (
        resolve_warehouse_id,
    )

    ag_id = str(ag.get("id") or "")
    # _l5_ag_rca_id is precomputed by the harness's earlier block at
    # 22650-22691; we recompute it here so the function is callable in
    # isolation.
    _l5_ag_rca_id = ""
    for _cid in (ag.get("source_cluster_ids") or []):
        _l5_ag_rca_id = str(iter_rca_id_by_cluster.get(str(_cid)) or "")
        if _l5_ag_rca_id:
            break

    for _drop in l5_ag_drops:
        _drop_cluster: dict | None = None
        _drop_root_cause = ""
        for _rc in (_drop.get("root_causes") or ()):
            if not _should_force_structural_synthesis(
                gate_drop_reason=(
                    "lever5_structural_sql_shape_no_example_sql"
                ),
                cluster_root_cause=str(_rc),
            ):
                continue
            for _cid in (_drop.get("source_clusters") or ()):
                _cand = iter_source_clusters_by_id.get(str(_cid))
                # BUG PRESERVED: strict equality between
                # _cand.get("root_cause") (RcaKind) and _rc (which
                # came from _LEVER5_GATE_DROPS[*].root_causes — prefers
                # asi_failure_type). Plan A fixes this; we preserve
                # it here so the regression test in Task 11 captures
                # the bug as a passing test.
                if isinstance(_cand, dict) and str(
                    _cand.get("root_cause") or ""
                ) == str(_rc):
                    _drop_cluster = dict(_cand)
                    _drop_root_cause = str(_rc)
                    break
            if _drop_cluster is not None:
                break
        if _drop_cluster is None:
            continue

        attempted.append(
            (str(_drop_cluster.get("cluster_id") or ""), _drop_root_cause)
        )

        _synth_result = synthesize(
            _drop_cluster,
            metadata_snapshot,
            benchmarks=benchmarks,
            catalog=catalog,
            gold_schema=schema,
            warehouse_id=resolve_warehouse_id(""),
            w=w,
            spark=spark,
        )
        if _synth_result.proposal is not None:
            _sp = _synth_result.proposal
            _forced_proposal = {
                "proposal_id": f"P{len(appended) + 1:03d}_FORCED",
                "cluster_id": f"{ag_id}_FORCED_SYN",
                "lever": 5,
                "scope": "genie_config",
                "patch_type": "add_example_sql",
                "change_description": (
                    f"[{ag_id}] Forced structural synthesis: "
                    f"{str(_sp.get('example_question', ''))[:80]}"
                ),
                "proposed_value": _sp.get("example_question", ""),
                "example_question": _sp.get("example_question", ""),
                "example_sql": _sp.get("example_sql", ""),
                "parameters": _sp.get("parameters", []) or [],
                "usage_guidance": _sp.get("usage_guidance", ""),
                "rationale": (
                    f"Forced structural synthesis at L5 gate "
                    f"drop (archetype="
                    f"{_sp.get('_archetype_name', '?')}). "
                    f"Root cause: {_drop_root_cause}."
                ),
                "confidence": 0.85,
                "questions_fixed": 1,
                "questions_at_risk": 0,
                "net_impact": 0.85,
                "kit_id": _sp.get("kit_id", ""),
                "target_qids": _sp.get("target_qids", []),
                "rca_id": _sp.get("rca_id", ""),
                "_archetype_name": _sp.get("_archetype_name", ""),
                "_cluster_id": _sp.get("_cluster_id", ""),
                "provenance": {
                    "synthesis_source": "forced_lever5_drop",
                    "drop_root_cause": _drop_root_cause,
                    "kit_id": _sp.get("kit_id", ""),
                    "target_qids": _sp.get("target_qids", []),
                },
            }
            appended.append(_forced_proposal)
            logger.info(
                "forced structural synthesis succeeded for AG=%s "
                "root_cause=%s archetype=%s",
                ag_id, _drop_root_cause,
                _sp.get("_archetype_name", "?"),
            )
        else:
            _nsc = no_structural_candidate_record(
                run_id=run_id,
                iteration=iteration,
                ag_id=str(ag_id),
                cluster_id=str(
                    _drop_cluster.get("cluster_id") or ""
                ),
                rca_id=_l5_ag_rca_id,
                root_cause=_drop_root_cause,
                target_qids=tuple(
                    str(q) for q in (
                        ag.get("affected_questions") or []
                    )
                    if str(q)
                ),
                attempted_archetypes=(
                    _synth_result.attempted_archetypes
                ),
            )
            emitted.append(_nsc.to_dict())
            logger.info(
                "forced structural synthesis produced no candidate "
                "for AG=%s root_cause=%s skipped=%s archetypes=%s",
                ag_id, _drop_root_cause,
                _synth_result.skipped_reason,
                _synth_result.attempted_archetypes,
            )

    return ForcedSynthesisDispatchResult(
        attempted_dispatches=tuple(attempted),
        appended_proposals=tuple(appended),
        emitted_decision_records=tuple(emitted),
    )
