"""Typed input/output models for the local lever-loop workbench.

These types live in the dev-only package and intentionally duplicate
nothing from ``genie_space_optimizer.models``. They describe what the
workbench *receives* (a normalized run bundle) and what it *emits*
(a per-stage funnel result and a recording of fake PATCH payloads).
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class WorkbenchProvenance:
    """Where the bundle came from. Used in the report header.

    ``source_kind`` is one of ``production_replay``, ``run_analysis``,
    or ``synthetic`` so the report makes it obvious whether a green
    result actually exercised production-shaped rows or a shape ladder.
    """

    source_kind: str
    source_run_id: str = ""
    source_artifacts: tuple[str, ...] = ()
    notes: str = ""

    def to_dict(self) -> dict:
        return {
            "source_kind": self.source_kind,
            "source_run_id": self.source_run_id,
            "source_artifacts": list(self.source_artifacts),
            "notes": self.notes,
        }


@dataclass(frozen=True)
class WorkbenchHardCase:
    """One sanitized hard QID admitted into the bundle.

    ``row`` is the canonical eval-row dict the state machine would see.
    ``typed_evidence`` is a dict snapshot of an upstream
    ``PerQidRcaEvidence`` carrier (or ``None`` when no typed evidence
    was captured). The workbench keeps it as a dict at the bundle layer
    and reconstructs the typed object on demand so the bundle stays
    JSON-trivial.
    """

    qid: str
    row: dict
    typed_evidence: dict | None = None
    expected_card_violations: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "qid": self.qid,
            "row": self.row,
            "typed_evidence": self.typed_evidence,
            "expected_card_violations": list(self.expected_card_violations),
        }


@dataclass(frozen=True)
class WorkbenchInputBundle:
    """Normalized input that the workbench runner consumes.

    A bundle carries one or more sanitized hard QID cases plus a
    minimal ``metadata_snapshot`` good enough for the applier gate.
    The bundle is serialisable as JSON via :meth:`to_json` so callers
    can hand-edit, archive, or replay it offline.

    Trial 16 v1.6 — ``post_apply_eval_tape`` extends bundles with
    canned post-apply eval rows that the workbench's
    ``_workbench_post_apply_eval_stub`` and ``ctx.post_apply_eval_rows``
    consume. The tape closes the workbench coverage gap from
    ``HARD_QID_SEEN → APPLIED`` (Trial 15) to
    ``HARD_QID_SEEN → ACCEPTED`` so the acceptance boundary the
    production postmortems failed at is now exercisable locally
    without a real benchmark run.
    """

    provenance: WorkbenchProvenance
    space_id: str
    hard_cases: tuple[WorkbenchHardCase, ...]
    metadata_snapshot: Mapping[str, Any] = field(default_factory=dict)
    # Each row mirrors an MLflow-flattened benchmark row: it MUST carry
    # ``question_id`` (or ``inputs/question_id`` / nested ``inputs``)
    # and ``feedback/result_correctness/value`` so the canonical
    # ``extract_question_id`` helper and ``_score`` reader on
    # acceptance_gate can join them to the patched qid.
    post_apply_eval_tape: tuple[Mapping[str, Any], ...] = ()

    @property
    def hard_qids(self) -> tuple[str, ...]:
        return tuple(c.qid for c in self.hard_cases)

    @property
    def eval_rows(self) -> tuple[dict, ...]:
        return tuple(dict(c.row) for c in self.hard_cases)

    def to_dict(self) -> dict:
        return {
            "provenance": self.provenance.to_dict(),
            "space_id": self.space_id,
            "metadata_snapshot": dict(self.metadata_snapshot),
            "hard_cases": [c.to_dict() for c in self.hard_cases],
            "post_apply_eval_tape": [dict(r) for r in self.post_apply_eval_tape],
        }

    def to_json(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2, default=str))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "WorkbenchInputBundle":
        prov_payload = payload.get("provenance") or {}
        provenance = WorkbenchProvenance(
            source_kind=str(prov_payload.get("source_kind") or "unknown"),
            source_run_id=str(prov_payload.get("source_run_id") or ""),
            source_artifacts=tuple(
                str(a) for a in prov_payload.get("source_artifacts") or ()
            ),
            notes=str(prov_payload.get("notes") or ""),
        )
        cases = tuple(
            WorkbenchHardCase(
                qid=str(c.get("qid") or ""),
                row=dict(c.get("row") or {}),
                typed_evidence=(
                    dict(c["typed_evidence"])
                    if isinstance(c.get("typed_evidence"), dict)
                    else None
                ),
                expected_card_violations=tuple(
                    str(v) for v in c.get("expected_card_violations") or ()
                ),
            )
            for c in payload.get("hard_cases") or ()
        )
        tape = tuple(
            dict(r) for r in (payload.get("post_apply_eval_tape") or ())
            if isinstance(r, Mapping)
        )
        return cls(
            provenance=provenance,
            space_id=str(payload.get("space_id") or ""),
            hard_cases=cases,
            metadata_snapshot=dict(payload.get("metadata_snapshot") or {}),
            post_apply_eval_tape=tape,
        )

    @classmethod
    def from_json_file(cls, path: Path) -> "WorkbenchInputBundle":
        return cls.from_dict(json.loads(Path(path).read_text()))


@dataclass(frozen=True)
class Stage1ProbeFinding:
    """Stage 1 preflight outcome for a single QID.

    Mirrors the keys ``Stage1InputEvidenceContract.field_sources``
    produces so the funnel report can show, per QID, which field
    sources were resolved and which violations would block Stage 1
    if a live LLM call were dispatched.
    """

    qid: str
    violations: tuple[str, ...]
    field_sources: Mapping[str, str]
    would_dispatch_llm: bool

    def to_dict(self) -> dict:
        return {
            "qid": self.qid,
            "violations": list(self.violations),
            "field_sources": dict(self.field_sources),
            "would_dispatch_llm": self.would_dispatch_llm,
        }


@dataclass(frozen=True)
class Stage1ProbeResult:
    """Aggregate Stage 1 preflight result for a bundle."""

    findings: tuple[Stage1ProbeFinding, ...]
    all_pass: bool

    def to_dict(self) -> dict:
        return {
            "all_pass": self.all_pass,
            "findings": [f.to_dict() for f in self.findings],
        }


@dataclass(frozen=True)
class RecordedPatch:
    """One PATCH the recording applier would have sent.

    Captures the structured fields the offline test fixtures
    (``FakeApiClient``) record so the workbench report can answer
    "what would the optimizer have shipped if we deployed?".
    """

    qid: str
    intent_id: str
    patch_type: str
    serialized_space: dict | None
    raw_body: dict | None

    def to_dict(self) -> dict:
        return {
            "qid": self.qid,
            "intent_id": self.intent_id,
            "patch_type": self.patch_type,
            "serialized_space": self.serialized_space,
            "raw_body": self.raw_body,
        }


@dataclass(frozen=True)
class StageProgress:
    """Per-QID progress through the funnel after a workbench run."""

    qid: str
    deepest_stage: str
    terminal_reason: str
    terminal_message: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class WorkbenchRunConfig:
    """Operator-facing configuration for one workbench run."""

    bundle_path: Path
    output_dir: Path
    llm_mode: str  # "live-databricks" | "live-llm-only" | "sm-tape" | "stage1-only"
    apply_mode: str = "fake-record"  # "fake-record" only in V1
    tape_path: Path | None = None
    profile: str | None = None  # Databricks CLI profile (live mode only)
    llm_model: str | None = None  # overrides LLM_MODEL env in live mode
    iteration: int = 1

    def to_dict(self) -> dict:
        return {
            "bundle_path": str(self.bundle_path),
            "output_dir": str(self.output_dir),
            "llm_mode": self.llm_mode,
            "apply_mode": self.apply_mode,
            "tape_path": str(self.tape_path) if self.tape_path else None,
            "profile": self.profile,
            "llm_model": self.llm_model,
            "iteration": self.iteration,
        }


@dataclass(frozen=True)
class WorkbenchRunResult:
    """Full result of one workbench run; rendered as JSON and Markdown."""

    config: WorkbenchRunConfig
    provenance: WorkbenchProvenance
    stage1_probe: Stage1ProbeResult
    stage_progress: tuple[StageProgress, ...]
    deepest_stage_reached: str
    markers: Mapping[str, Any]
    terminal_reasons: tuple[str, ...]
    recorded_patches: tuple[RecordedPatch, ...]
    surprises: tuple[str, ...]
    stdout_sample_lines: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "config": self.config.to_dict(),
            "provenance": self.provenance.to_dict(),
            "stage1_probe": self.stage1_probe.to_dict(),
            "stage_progress": [p.to_dict() for p in self.stage_progress],
            "deepest_stage_reached": self.deepest_stage_reached,
            "markers": dict(self.markers),
            "terminal_reasons": list(self.terminal_reasons),
            "recorded_patches": [p.to_dict() for p in self.recorded_patches],
            "surprises": list(self.surprises),
            "stdout_sample_lines": list(self.stdout_sample_lines),
        }


__all__ = [
    "RecordedPatch",
    "Stage1ProbeFinding",
    "Stage1ProbeResult",
    "StageProgress",
    "WorkbenchHardCase",
    "WorkbenchInputBundle",
    "WorkbenchProvenance",
    "WorkbenchRunConfig",
    "WorkbenchRunResult",
]
