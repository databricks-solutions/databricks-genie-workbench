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
    iter_summaries: list[Mapping[str, Any]] = []
    phase_b: list[Mapping[str, Any]] = []
    phase_b_no_records: list[Mapping[str, Any]] = []
    phase_a_artifact: list[Mapping[str, Any]] = []
    phase_b_artifact: list[Mapping[str, Any]] = []
    convergence: Mapping[str, Any] | None = None
    artifact_index: Mapping[str, Any] | None = None
    bundle_assembly_failed: list[Mapping[str, Any]] = []
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
        unknown={k: tuple(v) for k, v in unknown.items()},
        parse_errors=tuple(errors),
    )


def extract_replay_fixture(stdout: str) -> Mapping[str, Any] | None:
    begin = stdout.find(_FIXTURE_BEGIN)
    end = stdout.find(_FIXTURE_END)
    if begin < 0 or end < 0 or end <= begin:
        return None
    blob = stdout[begin + len(_FIXTURE_BEGIN) : end].strip()
    if not blob:
        return None
    try:
        parsed = json.loads(blob)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


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
