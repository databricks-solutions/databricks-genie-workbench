"""Pre-step Cycle 11: project ``_current_iter_inputs`` to invariants evidence.

The Cycle 11 invariant runner at ``harness.py:_run_iteration_invariants_and_append_records``
formerly built an empty evidence literal, so I2/I3/I4/I7 always
saw ``iterations: []`` and emitted nothing. This module's
``project_iter_evidence`` reads the live in-iteration capture
populated by ``begin_iteration_capture`` plus the prior-iteration
evidence dict (for I4 history) and produces a populated evidence
dict that exercises I1 through I7.

Pure, no I/O. The harness wrapper is the only caller and is
responsible for honoring ``loop_invariants_enabled`` /
``loop_invariants_strict`` and translating violations to
``DecisionRecord``s. Manifest, replay_validation, and
final_iteration_journey_hard_qids are intentionally projected as
empty here — those are run-end signals owned by Phase H and feed
the run-end runner, not the per-iteration runner.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

_ACCEPTANCE_DECISION_TYPES: tuple[str, ...] = (
    "control_plane_acceptance",
    "control_plane_acceptance_decision",
    "acceptance_decision",
)


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _coerce_str_list(value: Any) -> list[str]:
    if not value:
        return []
    out: list[str] = []
    for item in value:
        s = str(item)
        if s:
            out.append(s)
    return out


def _record_to_mapping(record: Any) -> dict[str, Any] | None:
    """B2 (2026-05-13) — normalize a decision record to a plain dict.

    Branch order (each branch mutually exclusive with the next):

    1. Already a Mapping (dict / MappingProxyType / etc.) → ``dict(record)``.
    2. Exposes a callable ``to_dict()`` (the ``DecisionRecord`` dataclass
       contract at ``rca_decision_trace.py:476``) → invoke and coerce.
       If ``to_dict()`` raises or returns a non-Mapping, return ``None``
       so the caller drops the record (defensive — a buggy dataclass must
       not propagate to invariants).
    3. Anything else → return ``None``.

    Closes 18 medium-tier ``I_CHECK_FAILED`` violations from 2314bb2c
    where ``check_i7_rca_grounding`` and ``check_i14_l6_decline_dedup``
    raised ``AttributeError("'DecisionRecord' object has no attribute
    'get'")`` because some emit sites append the dataclass directly to
    ``current_iter_inputs["decision_records"]``.

    Evidence anchor:
    docs/runid_analysis/2314bb2c-95a1-4d60-8226-09e5155aee2a/postmortem.md F7
    """
    if isinstance(record, Mapping):
        return dict(record)
    to_dict_attr = getattr(record, "to_dict", None)
    if callable(to_dict_attr):
        try:
            result = to_dict_attr()
        except Exception:
            return None
        if isinstance(result, Mapping):
            return dict(result)
        return None
    return None


def _project_clusters(
    current_iter_inputs: Mapping[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in (current_iter_inputs.get("clusters") or ()):
        if not isinstance(c, Mapping):
            continue
        cid = str(c.get("cluster_id") or "")
        if not cid:
            continue
        out.append({
            "cluster_id": cid,
            "recommended_levers": [
                int(x) for x in (c.get("recommended_levers") or [])
                if str(x).strip().isdigit()
            ],
            "qids": _coerce_str_list(c.get("qids")),
        })
    return out


def _project_ags(
    current_iter_inputs: Mapping[str, Any],
) -> list[dict[str, Any]]:
    sr = current_iter_inputs.get("strategist_response") or {}
    if not isinstance(sr, Mapping):
        return []
    out: list[dict[str, Any]] = []
    for ag in (sr.get("action_groups") or ()):
        if not isinstance(ag, Mapping):
            continue
        ag_id = str(ag.get("id") or ag.get("ag_id") or "")
        if not ag_id:
            continue
        levers_list = [
            int(x) for x in (
                ag.get("Levers") or ag.get("levers") or []
            )
            if str(x).strip().isdigit()
        ]
        out.append({
            "id": ag_id,
            "Levers": levers_list,
            "levers": levers_list,
            "source_cluster_ids": _coerce_str_list(
                ag.get("source_cluster_ids")
            ),
            "lever_directives": dict(ag.get("lever_directives") or {}),
            "root_cause": str(ag.get("root_cause") or ""),
        })
    return out


def _project_acceptance_decision(
    current_iter_inputs: Mapping[str, Any],
) -> dict[str, Any]:
    """Find the latest acceptance-decision record in this iteration."""
    records = current_iter_inputs.get("decision_records") or ()
    found: dict[str, Any] = {}
    for r in records:
        if not isinstance(r, Mapping):
            continue
        if str(r.get("decision_type") or "") in _ACCEPTANCE_DECISION_TYPES:
            found = dict(r)
    return found


def _project_applied_patches(
    current_iter_inputs: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Read applied patches off ``ag_outcomes`` and ``decision_records``.

    The harness writes ``ag_outcomes[ag_id]`` as a status string, so we
    cross-reference patch-applied decision records to recover ``lever``
    per applied patch. Records may carry ``decision_type=patch_applied``
    or ``decision_type=patch_apply``.
    """
    out: list[dict[str, Any]] = []
    for r in (current_iter_inputs.get("decision_records") or ()):
        if not isinstance(r, Mapping):
            continue
        dt = str(r.get("decision_type") or "")
        if dt not in {"patch_applied", "patch_apply"}:
            continue
        ag_id = str(r.get("ag_id") or "")
        lever = r.get("lever")
        if not ag_id or lever is None:
            continue
        try:
            lever_int = int(lever)
        except (TypeError, ValueError):
            continue
        out.append({
            "ag_id": ag_id,
            "lever": lever_int,
            "body_fingerprint": str(r.get("body_fingerprint") or ""),
        })
    return out


def _project_iteration(
    *,
    current_iter_inputs: Mapping[str, Any],
    iteration: int,
) -> dict[str, Any]:
    ags = _project_ags(current_iter_inputs)
    applied = _project_applied_patches(current_iter_inputs)
    selected_ag_id = ags[0]["id"] if ags else ""
    fingerprints = sorted({
        p["body_fingerprint"] for p in applied
        if str(p.get("ag_id") or "") == selected_ag_id
        and p.get("body_fingerprint")
    })
    open_hard = [
        c["cluster_id"] for c in _project_clusters(current_iter_inputs)
    ]
    rca_present_raw = current_iter_inputs.get("rca_cards_present") or {}
    rca_present = {
        str(k): bool(v) for k, v in dict(rca_present_raw).items()
    }
    _decision_records = list(
        current_iter_inputs.get("decision_records") or []
    )
    # Cycle 16 T5 — I11 evidence keys (causal continuity).
    # Count typed records in this iteration's decision_records;
    # halt AGs from the per-iter set stamped during blast-radius gate.
    _c16_structural_dropped_count = sum(
        1 for r in _decision_records
        if isinstance(r, Mapping)
        and r.get("reason_code") == "structural_causal_dropped"
    )
    _c16_branch_c_synthesized_count = sum(
        1 for r in _decision_records
        if isinstance(r, Mapping)
        and r.get("decision_type")
        == "narrow_replacement_branch_c_synthesized"
    )
    _c16_no_structural_alternative_ag_ids = tuple(
        str(a) for a in (
            current_iter_inputs.get(
                "_c16_no_structural_alternative_ags"
            ) or ()
        )
    )
    return {
        "iteration": int(iteration),
        "clusters": _project_clusters(current_iter_inputs),
        "ags": ags,
        "applied_patches": applied,
        "selected_ag_id": selected_ag_id,
        "proposal_count": _coerce_int(
            current_iter_inputs.get("proposal_count")
        ),
        "applied_patch_body_fingerprints": fingerprints,
        "acceptance_decision": _project_acceptance_decision(
            current_iter_inputs
        ),
        "open_hard_cluster_ids": open_hard,
        "rca_cards_present": rca_present,
        "decision_records": _decision_records,
        # Cycle 16 T5 — I11 causal-continuity evidence keys.
        "structural_causal_dropped_count": _c16_structural_dropped_count,
        "narrow_branch_c_synthesized_count": _c16_branch_c_synthesized_count,
        "no_structural_alternative_ag_ids": _c16_no_structural_alternative_ag_ids,
    }


def project_iter_evidence(
    *,
    current_iter_inputs: Mapping[str, Any],
    iteration: int,
    run_id: str,
    iter_producer_exceptions: Mapping[str, Any] | None,
    prior_iter_evidence: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Project the live iteration capture into invariants evidence.

    Args:
        current_iter_inputs: The dict allocated by
            ``begin_iteration_capture`` and mutated in place during
            this iteration.
        iteration: The 1-indexed iteration counter.
        run_id: Optimizer run id; empty string short-circuits to a
            no-op evidence dict so the harness wrapper's existing
            "skip when run_id is blank" contract is preserved.
        iter_producer_exceptions: Per-producer exception map for
            ``phase_b.producer_exceptions``.
        prior_iter_evidence: The previous iteration's projection,
            used to grow the ``iterations`` list so I4 (no silent
            retry) sees prev+curr in one evidence dict.

    Pure: never mutates ``current_iter_inputs`` or
    ``prior_iter_evidence``.
    """
    phase_b = {
        "total_records": len(
            current_iter_inputs.get("decision_records") or []
        ),
        "producer_exceptions": dict(iter_producer_exceptions or {}),
    }
    base: dict[str, Any] = {
        "phase_b": phase_b,
        "replay_fixture_records": 0,
        "iterations": [],
        "manifest": {"declared_paths": [], "materialized_paths": []},
        "convergence": {},
    }
    if not run_id:
        return base
    prior_iters = []
    if prior_iter_evidence and isinstance(prior_iter_evidence, Mapping):
        for it in (prior_iter_evidence.get("iterations") or ()):
            if isinstance(it, Mapping):
                prior_iters.append(dict(it))
    base["iterations"] = prior_iters + [
        _project_iteration(
            current_iter_inputs=current_iter_inputs,
            iteration=iteration,
        )
    ]
    return base
