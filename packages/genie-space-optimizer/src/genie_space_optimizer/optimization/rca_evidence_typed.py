"""Plan 3 — typed RCA evidence carrier + deterministic helpers.

Three public symbols:

  * ``PerQidRcaEvidence`` — frozen+slots+JsonRoundTrip dataclass that
    travels through ``RcaEvidenceBundle.per_qid_evidence_typed`` and
    is read by Plan 4+ consumers without dict-probing.
  * ``rca_kind_from_repair_family`` — deterministic mapper from the
    LLM's open-vocab ``suggested_repair_family`` to the closed
    ``RcaKind`` enum (best-effort substring match; UNKNOWN when no
    family matches).
  * ``PerQidRcaEvidence.to_legacy_dict`` — projector from typed to
    the legacy ``per_qid_evidence`` dict shape every existing
    downstream consumer expects.

This module imports neither the extractor nor the stage — it is the
unidirectional type / helper layer.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True, slots=True)
class PerQidRcaEvidence(JsonRoundTrip):
    """Typed per-qid RCA evidence — wire-stable carrier through stage
    I/O.

    Field names match ``PerQidRcaEvidenceOutput`` exactly; the
    Pydantic/dataclass alignment is pinned by
    ``test_per_qid_rca_evidence_pydantic_dataclass_alignment.py``.

    Why a tuple, not list, for sequence fields: frozen-dataclass
    semantics require hashable members. ``to_legacy_dict`` converts
    back to lists for the legacy consumers.
    """

    qid: str
    observed_failure: str
    generated_sql_issue: str
    expected_sql_shape: str
    blame_set: tuple[str, ...]
    suggested_repair_family: str
    repair_hint_patch_type: PatchType
    confidence: Literal["high", "medium", "low"]
    quoted_evidence: tuple[str, ...]
