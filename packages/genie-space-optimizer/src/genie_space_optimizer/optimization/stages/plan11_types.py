"""Plan 11 — typed carriers for the three LLM stages and validation pipeline.

PerQidDiagnosis   : Stage 1 output — per-QID diagnosis with free-text rca_kind_label.
FailureCluster    : Stage 2 output — cluster with free-text repair_hypothesis.
ValidationError   : one validator rejection, typed by error_kind.
ValidationResult  : envelope of all errors (or empty) for one patch.

All four subclass JsonRoundTrip so Delta and replay round-trips work identically
to Plan 1/5/9 carriers.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True, slots=True)
class PerQidDiagnosis(JsonRoundTrip):
    qid: str
    rca_kind_label: str
    observed_failure: str
    generated_sql_issue: str
    expected_sql_shape: str
    blame_set: tuple[str, ...]
    evidence_summary: str
    confidence: Literal["high", "medium", "low"]
    # Trial 19 B5 — free-text repair-intent label emitted by Stage 1.
    # Default empty string for back-compat with pre-Trial-19 Delta rows
    # so ``from_json`` is byte-stable on old payloads. Consumed by the
    # Stage 3 prompt and ``rca_card_builder.intended_patch_shape_for_
    # root_cause`` (B2) as the authoritative repair-intent label.
    intended_patch_shape: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "qid": self.qid,
            "rca_kind_label": self.rca_kind_label,
            "observed_failure": self.observed_failure,
            "generated_sql_issue": self.generated_sql_issue,
            "expected_sql_shape": self.expected_sql_shape,
            "blame_set": list(self.blame_set),
            "evidence_summary": self.evidence_summary,
            "confidence": self.confidence,
            # Trial 19 B5 — round-trip the LLM-emitted intent.
            "intended_patch_shape": self.intended_patch_shape,
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "PerQidDiagnosis":
        return cls(
            qid=str(payload["qid"]),
            rca_kind_label=str(payload.get("rca_kind_label", "")),
            observed_failure=str(payload.get("observed_failure", "")),
            generated_sql_issue=str(payload.get("generated_sql_issue", "")),
            expected_sql_shape=str(payload.get("expected_sql_shape", "")),
            blame_set=tuple(str(b) for b in payload.get("blame_set", [])),
            evidence_summary=str(payload.get("evidence_summary", "")),
            confidence=payload.get("confidence", "low"),  # type: ignore[arg-type]
            # Trial 19 B5 — empty default keeps pre-Trial-19 rows valid.
            intended_patch_shape=str(payload.get("intended_patch_shape", "")),
        )


@dataclass(frozen=True, slots=True)
class FailureCluster(JsonRoundTrip):
    cluster_id: str
    semantic_theme: str
    member_qids: tuple[str, ...]
    unifying_evidence: str
    repair_hypothesis: str
    primary_blame_set: tuple[str, ...]
    confidence: Literal["high", "medium", "low"]
    # Trial 23 W4 — the closed-enum RCA label Stage 1's diagnosis emitted
    # (``PerQidDiagnosis.rca_kind_label``). Threaded onto the Stage 3
    # cluster so the KIT_FOR_RCA validator and the W4 RCA-to-mechanism
    # router have the RCA kind to route on. Empty default keeps every
    # pre-Trial-23 caller and replay fixture byte-stable.
    root_cause: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "semantic_theme": self.semantic_theme,
            "member_qids": list(self.member_qids),
            "unifying_evidence": self.unifying_evidence,
            "repair_hypothesis": self.repair_hypothesis,
            "primary_blame_set": list(self.primary_blame_set),
            "confidence": self.confidence,
            "root_cause": self.root_cause,
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "FailureCluster":
        return cls(
            cluster_id=str(payload["cluster_id"]),
            semantic_theme=str(payload.get("semantic_theme", "")),
            member_qids=tuple(str(q) for q in payload.get("member_qids", [])),
            unifying_evidence=str(payload.get("unifying_evidence", "")),
            repair_hypothesis=str(payload.get("repair_hypothesis", "")),
            primary_blame_set=tuple(
                str(b) for b in payload.get("primary_blame_set", [])
            ),
            confidence=payload.get("confidence", "low"),  # type: ignore[arg-type]
            root_cause=str(payload.get("root_cause", "")),
        )


_VALID_ERROR_KINDS = frozenset(
    {
        "genie_schema",
        "asset_reference",
        "sql_execution",
        "instruction_canonical",
        "patch_type_unknown",
        "patch_body_missing_field",
    }
)


@dataclass(frozen=True, slots=True)
class ValidationError(JsonRoundTrip):
    patch_id: str
    error_kind: Literal[
        "genie_schema",
        "asset_reference",
        "sql_execution",
        "instruction_canonical",
        "patch_type_unknown",
        "patch_body_missing_field",
    ]
    error_detail: str
    failing_location: str | None

    def to_json(self) -> dict[str, Any]:
        return {
            "patch_id": self.patch_id,
            "error_kind": self.error_kind,
            "error_detail": self.error_detail[:2048],
            "failing_location": self.failing_location,
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "ValidationError":
        return cls(
            patch_id=str(payload["patch_id"]),
            error_kind=payload["error_kind"],  # type: ignore[arg-type]
            error_detail=str(payload.get("error_detail", "")),
            failing_location=payload.get("failing_location"),
        )


@dataclass(frozen=True, slots=True)
class ValidationResult(JsonRoundTrip):
    patch_id: str
    is_valid: bool
    errors: tuple[ValidationError, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "patch_id": self.patch_id,
            "is_valid": self.is_valid,
            "errors": [e.to_json() for e in self.errors],
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "ValidationResult":
        return cls(
            patch_id=str(payload["patch_id"]),
            is_valid=bool(payload["is_valid"]),
            errors=tuple(
                ValidationError.from_json(e) for e in payload.get("errors", [])
            ),
        )
