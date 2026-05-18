"""Parse stable ``GSO_*_V1`` stdout markers into typed records.

Pure functions only — no I/O. The contract for emission lives in
``optimization/run_analysis_contract.py``; this module is the
read-side counterpart.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping

_MARKER_RE = re.compile(r"^(GSO_[A-Z0-9_]+_V\d+)\s+(.+)$")
_FIXTURE_BEGIN = "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN==="
_FIXTURE_END = "===PHASE_A_REPLAY_FIXTURE_JSON_END==="


@dataclass(frozen=True)
class MarkerLog:
    run_manifest: Mapping[str, Any] | None
    iteration_summaries: tuple[Mapping[str, Any], ...]
    phase_b: tuple[Mapping[str, Any], ...]
    phase_b_no_records: tuple[Mapping[str, Any], ...]
    phase_a_artifact: tuple[Mapping[str, Any], ...]
    phase_b_artifact: tuple[Mapping[str, Any], ...]
    convergence: Mapping[str, Any] | None
    artifact_index: Mapping[str, Any] | None = None
    bundle_assembly_failed: tuple[Mapping[str, Any], ...] = ()
    plateau_input_source: tuple[Mapping[str, Any], ...] = ()
    run_manifest_v2: Mapping[str, Any] | None = None
    phase_h_strict_validation: Mapping[str, Any] | None = None
    contract_health: Mapping[str, Any] | None = None  # RCO-2a
    bundle_assembly_incomplete: tuple[Mapping[str, Any], ...] | None = None
    # Cycle 14B-T3 — patch-subset isolation diagnostic + outcome.
    # Tuple-typed because the orchestrator may emit the diagnostic
    # marker once per AG-emit-with-rejection within a run.
    patch_isolation_diagnostic: tuple[Mapping[str, Any], ...] | None = None
    patch_isolation_outcome: tuple[Mapping[str, Any], ...] | None = None
    # Cycle 14-C T5 — diagnostic ``GSO_ATTRIBUTION_DRIFT_V1`` payloads,
    # one per accepted_with_attribution_drift acceptance.
    attribution_drift: tuple[Mapping[str, Any], ...] = ()
    unknown: Mapping[str, tuple[Mapping[str, Any], ...]] = field(default_factory=dict)
    parse_errors: tuple[str, ...] = field(default_factory=tuple)

    def optimization_run_id(self) -> str | None:
        if self.run_manifest is not None:
            value = self.run_manifest.get("optimization_run_id")
            if isinstance(value, str) and value:
                return value
        for source in (self.iteration_summaries, self.phase_b, self.phase_b_artifact):
            for entry in source:
                value = entry.get("optimization_run_id")
                if isinstance(value, str) and value:
                    return value
        return None


def parse_markers(stdout: str) -> MarkerLog:
    run_manifest: Mapping[str, Any] | None = None
    run_manifest_v2: Mapping[str, Any] | None = None
    iter_summaries: list[Mapping[str, Any]] = []
    phase_b: list[Mapping[str, Any]] = []
    phase_b_no_records: list[Mapping[str, Any]] = []
    phase_a_artifact: list[Mapping[str, Any]] = []
    phase_b_artifact: list[Mapping[str, Any]] = []
    convergence: Mapping[str, Any] | None = None
    artifact_index: Mapping[str, Any] | None = None
    bundle_assembly_failed: list[Mapping[str, Any]] = []
    plateau_input_source: list[Mapping[str, Any]] = []
    phase_h_strict_validation: Mapping[str, Any] | None = None
    contract_health: Mapping[str, Any] | None = None  # RCO-2a
    bundle_assembly_incomplete: list[Mapping[str, Any]] | None = None
    patch_isolation_diagnostic: list[Mapping[str, Any]] | None = None
    patch_isolation_outcome: list[Mapping[str, Any]] | None = None
    attribution_drift: list[Mapping[str, Any]] = []
    unknown: dict[str, list[Mapping[str, Any]]] = {}
    errors: list[str] = []

    for line in stdout.splitlines():
        match = _MARKER_RE.match(line.strip())
        if not match:
            continue
        name, raw = match.group(1), match.group(2)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            errors.append(f"{name}: invalid json")
            continue
        if not isinstance(payload, dict):
            errors.append(f"{name}: payload not an object")
            continue
        if name == "GSO_RUN_MANIFEST_V1":
            run_manifest = payload
        elif name == "GSO_RUN_MANIFEST_V2":
            run_manifest_v2 = payload
        elif name == "GSO_ITERATION_SUMMARY_V1":
            iter_summaries.append(payload)
        elif name == "GSO_PHASE_B_V1":
            phase_b.append(payload)
        elif name == "GSO_PHASE_B_NO_RECORDS_V1":
            phase_b_no_records.append(payload)
        elif name == "GSO_PHASE_A_ARTIFACT_V1":
            phase_a_artifact.append(payload)
        elif name == "GSO_PHASE_B_ARTIFACT_V1":
            phase_b_artifact.append(payload)
        elif name == "GSO_CONVERGENCE_V1":
            convergence = payload
        elif name == "GSO_ARTIFACT_INDEX_V1":
            artifact_index = payload
        elif name == "GSO_BUNDLE_ASSEMBLY_FAILED_V1":
            bundle_assembly_failed.append(payload)
        elif name == "GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1":
            if bundle_assembly_incomplete is None:
                bundle_assembly_incomplete = []
            bundle_assembly_incomplete.append(payload)
        elif name == "GSO_PLATEAU_INPUT_SOURCE_V1":
            plateau_input_source.append(payload)
        elif name == "GSO_PHASE_H_STRICT_VALIDATION_V1":
            phase_h_strict_validation = payload
        elif name == "GSO_CONTRACT_HEALTH_V1":
            contract_health = payload  # last-wins, matches phase_h convention
        elif name == "GSO_PATCH_ISOLATION_DIAGNOSTIC_V1":
            if patch_isolation_diagnostic is None:
                patch_isolation_diagnostic = []
            patch_isolation_diagnostic.append(payload)
        elif name == "GSO_PATCH_ISOLATION_OUTCOME_V1":
            if patch_isolation_outcome is None:
                patch_isolation_outcome = []
            patch_isolation_outcome.append(payload)
        elif name == "GSO_ATTRIBUTION_DRIFT_V1":
            attribution_drift.append(payload)
        else:
            unknown.setdefault(name, []).append(payload)

    return MarkerLog(
        run_manifest=run_manifest,
        iteration_summaries=tuple(iter_summaries),
        phase_b=tuple(phase_b),
        phase_b_no_records=tuple(phase_b_no_records),
        phase_a_artifact=tuple(phase_a_artifact),
        phase_b_artifact=tuple(phase_b_artifact),
        convergence=convergence,
        artifact_index=artifact_index,
        bundle_assembly_failed=tuple(bundle_assembly_failed),
        plateau_input_source=tuple(plateau_input_source),
        run_manifest_v2=run_manifest_v2,
        phase_h_strict_validation=phase_h_strict_validation,
        contract_health=contract_health,
        bundle_assembly_incomplete=(
            tuple(bundle_assembly_incomplete)
            if bundle_assembly_incomplete is not None else None
        ),
        patch_isolation_diagnostic=(
            tuple(patch_isolation_diagnostic)
            if patch_isolation_diagnostic is not None else None
        ),
        patch_isolation_outcome=(
            tuple(patch_isolation_outcome)
            if patch_isolation_outcome is not None else None
        ),
        attribution_drift=tuple(attribution_drift),
        unknown={k: tuple(v) for k, v in unknown.items()},
        parse_errors=tuple(errors),
    )


def extract_replay_fixture(stdout: str) -> Mapping[str, Any] | None:
    """WU-5 (2026-05-18) — delegate to ``replay_fixture_marker`` so
    both plain-JSON and base64-fallback channels are honoured.

    Returns the parsed payload or ``None`` when neither channel
    produced a parseable fixture. Backward compatible with legacy
    plain-only emissions (the new extractor's first branch handles
    them). Postmortems that previously failed with
    ``extractor_succeeded=False, json_parseable=False,
    contains_prompt_source=True`` now recover via the base64
    fallback when ``_run_lever_loop`` is updated to call
    ``emit_dual_fixture``.
    """
    from genie_space_optimizer.optimization.replay_fixture_marker import (
        extract_replay_fixture_from_stream,
    )
    result = extract_replay_fixture_from_stream(stdout)
    payload = result.payload
    return payload if isinstance(payload, dict) else None


def _parse_named_marker(line: str, expected_name: str) -> dict:
    """Parse a single ``GSO_*_V1 {json}`` line emitted by
    ``run_analysis_contract.marker_line`` and return the JSON payload.

    Raises ``ValueError`` when the line does not start with the
    expected marker name or the JSON payload is invalid.
    """
    stripped = line.strip()
    match = _MARKER_RE.match(stripped)
    if not match:
        raise ValueError(f"not a GSO marker line: {line!r}")
    name, raw = match.group(1), match.group(2)
    if name != expected_name:
        raise ValueError(f"expected {expected_name}, got {name}")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"{expected_name}: payload not an object")
    return payload


def parse_proposal_generation_empty_marker(line: str) -> dict:
    """Parse ``GSO_PROPOSAL_GENERATION_EMPTY_V1 {json}``.

    Returns ``{"ag_id", "iteration", "target_qids"}``. Raises
    ``ValueError`` if the line does not match.
    """
    payload = _parse_named_marker(line, "GSO_PROPOSAL_GENERATION_EMPTY_V1")
    return {
        "ag_id": str(payload.get("ag_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "target_qids": list(payload.get("target_qids") or []),
    }


def parse_structural_gate_dropped_marker(line: str) -> dict:
    """Parse ``GSO_STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY_V1 {json}``.

    Returns ``{"ag_id", "iteration", "root_causes", "target_qids"}``.
    """
    payload = _parse_named_marker(
        line, "GSO_STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY_V1"
    )
    return {
        "ag_id": str(payload.get("ag_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "root_causes": list(payload.get("root_causes") or []),
        "target_qids": list(payload.get("target_qids") or []),
    }


def parse_no_structural_candidate_marker(line: str) -> dict:
    """Parse ``GSO_NO_STRUCTURAL_CANDIDATE_V1 {json}``.

    Returns ``{"ag_id", "iteration", "attempted_archetypes"}``.
    """
    payload = _parse_named_marker(line, "GSO_NO_STRUCTURAL_CANDIDATE_V1")
    return {
        "ag_id": str(payload.get("ag_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "attempted_archetypes": list(payload.get("attempted_archetypes") or []),
    }


def parse_proposal_failure_decided_marker(line: str) -> dict:
    """Plan P-F — parse ``GSO_PROPOSAL_FAILURE_DECIDED_V1 {json}``.

    Returns ``{"ag_id", "iteration", "failure_mode", "next_action",
    "cluster_signature", "prior_failure_count"}``. Raises
    ``ValueError`` if the line does not match.
    """
    payload = _parse_named_marker(line, "GSO_PROPOSAL_FAILURE_DECIDED_V1")
    return {
        "ag_id": str(payload.get("ag_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "failure_mode": str(payload.get("failure_mode") or ""),
        "next_action": str(payload.get("next_action") or ""),
        "cluster_signature": str(payload.get("cluster_signature") or ""),
        "prior_failure_count": int(payload.get("prior_failure_count") or 0),
    }


def parse_iteration_budget_marker(line: str) -> dict:
    """Parse ``GSO_ITERATION_BUDGET_V1 {json}`` (Cycle 5 T1).

    Returns ``{"optimization_run_id", "iteration", "consumed",
    "no_op_cause", "applied_patches", "iteration_counter_after"}``.
    """
    payload = _parse_named_marker(line, "GSO_ITERATION_BUDGET_V1")
    return {
        "optimization_run_id": str(payload.get("optimization_run_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "consumed": bool(payload.get("consumed")),
        "no_op_cause": str(payload.get("no_op_cause") or ""),
        "applied_patches": int(payload.get("applied_patches") or 0),
        "iteration_counter_after": int(
            payload.get("iteration_counter_after") or 0
        ),
    }


def parse_lever6_forced_marker(line: str) -> dict:
    """Parse ``GSO_LEVER6_FORCED_V1 {json}`` (Cycle 7 N3).

    Returns ``{"optimization_run_id", "iteration", "ag_id",
    "cluster_id", "root_cause", "target_qids", "recommended_levers",
    "existing_patch_types"}``. Missing fields default to empty strings,
    zero, or empty lists so older fixtures parse cleanly.
    """
    payload = _parse_named_marker(line, "GSO_LEVER6_FORCED_V1")
    return {
        "optimization_run_id": str(payload.get("optimization_run_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "ag_id": str(payload.get("ag_id") or ""),
        "cluster_id": str(payload.get("cluster_id") or ""),
        "root_cause": str(payload.get("root_cause") or ""),
        "target_qids": [
            str(q) for q in (payload.get("target_qids") or [])
        ],
        "recommended_levers": [
            int(L) for L in (payload.get("recommended_levers") or [])
        ],
        "existing_patch_types": [
            str(p) for p in (payload.get("existing_patch_types") or [])
        ],
    }


def parse_lever6_force_llm_declined_marker(line: str) -> dict:
    """Parse ``GSO_LEVER6_FORCE_LLM_DECLINED_V1 {json}`` (Cycle 10 W3).

    Returns ``{"run_id", "iteration", "ag_id", "cluster_id",
    "root_cause"}``.
    """
    payload = _parse_named_marker(line, "GSO_LEVER6_FORCE_LLM_DECLINED_V1")
    return {
        "run_id": str(payload.get("run_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "ag_id": str(payload.get("ag_id") or ""),
        "cluster_id": str(payload.get("cluster_id") or ""),
        "root_cause": str(payload.get("root_cause") or ""),
    }


def parse_lever6_force_raised_marker(line: str) -> dict:
    """Parse ``GSO_LEVER6_FORCE_RAISED_V1 {json}`` (Cycle 10 W3).

    Returns ``{"run_id", "iteration", "ag_id", "cluster_id",
    "root_cause", "exception_repr"}``.
    """
    payload = _parse_named_marker(line, "GSO_LEVER6_FORCE_RAISED_V1")
    return {
        "run_id": str(payload.get("run_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "ag_id": str(payload.get("ag_id") or ""),
        "cluster_id": str(payload.get("cluster_id") or ""),
        "root_cause": str(payload.get("root_cause") or ""),
        "exception_repr": str(payload.get("exception_repr") or ""),
    }


def parse_narrow_not_applicable_marker(line: str) -> dict:
    """Parse ``GSO_NARROW_NOT_APPLICABLE_V1 {json}`` (Cycle 10 W4).

    Returns ``{"run_id", "iteration", "ag_id", "cluster_id",
    "root_cause", "original_patch_type", "reason"}``.
    """
    payload = _parse_named_marker(line, "GSO_NARROW_NOT_APPLICABLE_V1")
    return {
        "run_id": str(payload.get("run_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "ag_id": str(payload.get("ag_id") or ""),
        "cluster_id": str(payload.get("cluster_id") or ""),
        "root_cause": str(payload.get("root_cause") or ""),
        "original_patch_type": str(payload.get("original_patch_type") or ""),
        "reason": str(payload.get("reason") or ""),
    }


def parse_ag_levers_unioned_marker(line: str) -> dict:
    """Parse ``GSO_AG_LEVERS_UNIONED_V1 {json}`` (Cycle 10 W8).

    Returns ``{"run_id", "iteration", "ag_id", "cluster_id",
    "levers_before", "levers_after"}``.
    """
    payload = _parse_named_marker(line, "GSO_AG_LEVERS_UNIONED_V1")
    return {
        "run_id": str(payload.get("run_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "ag_id": str(payload.get("ag_id") or ""),
        "cluster_id": str(payload.get("cluster_id") or ""),
        "levers_before": [str(l) for l in (payload.get("levers_before") or [])],
        "levers_after": [str(l) for l in (payload.get("levers_after") or [])],
    }


def parse_plateau_input_source_marker(line: str) -> dict:
    """Parse ``GSO_PLATEAU_INPUT_SOURCE_V1 {json}`` (Cycle 11 Task 15).

    Returns ``{"optimization_run_id", "iteration", "source",
    "qids_count", "last_acceptance_was_rollback"}``.
    """
    payload = _parse_named_marker(line, "GSO_PLATEAU_INPUT_SOURCE_V1")
    return {
        "optimization_run_id": str(payload.get("optimization_run_id") or ""),
        "iteration": int(payload.get("iteration") or 0),
        "source": str(payload.get("source") or ""),
        "qids_count": int(payload.get("qids_count") or 0),
        "last_acceptance_was_rollback": bool(
            payload.get("last_acceptance_was_rollback")
        ),
    }


def parse_candidate_ledger_entry_marker(line: str) -> dict:
    """Parse ``GSO_CANDIDATE_LEDGER_ENTRY_V1 {json}`` (Phase 0.4).

    Returns ``{"optimization_run_id", "entry"}`` where ``entry`` is
    the full ledger payload (22 required fields + schema_version).
    """
    payload = _parse_named_marker(line, "GSO_CANDIDATE_LEDGER_ENTRY_V1")
    return {
        "optimization_run_id": str(payload.get("optimization_run_id") or ""),
        "entry": dict(payload.get("entry") or {}),
    }


def extract_candidate_ledger_from_stdout(stdout: str) -> tuple[dict, ...]:
    """Phase 0.4 — convenience reader that returns every embedded
    ledger entry from a stdout blob in iteration order.

    Used by the postmortem skill when no JSONL artifact is in the
    bundle (e.g., stale-anchor case).
    """
    out: list[dict] = []
    for raw in stdout.splitlines():
        if not raw.startswith("GSO_CANDIDATE_LEDGER_ENTRY_V1 "):
            continue
        try:
            parsed = parse_candidate_ledger_entry_marker(raw)
        except ValueError:
            continue
        out.append(parsed["entry"])
    return tuple(out)
