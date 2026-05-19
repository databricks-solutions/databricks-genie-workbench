"""Plan 5 — LLM-driven repair intent synthesizer.

Public surface (filled by Tasks 7-8):
  * ``synthesize_repair_intent_for_cluster(...)`` — one-LLM-call-per-
    cluster driver returning ``RepairProposal | None``.

Private helpers (this task, Task 6):
  * ``_validate_patch_body_against_patch_type``
  * ``_validate_blame_set_in_identifier_allowlist``
  * ``_validate_benchmark_leakage_relaxed_for_other``
  * ``_stamp_intent_id``
"""
from __future__ import annotations

import logging
import re
from typing import Any

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    PatchBodyValidationError,
    RepairProposal,
    required_patch_body_fields,
)

logger = logging.getLogger(__name__)


def _validate_patch_body_against_patch_type(
    proposal: RepairProposal,
) -> None:
    """Reject when patch_body is missing a required field for its
    patch_type. Per-patch-type required fields live in
    ``repair_proposal_typed._REQUIRED_PATCH_BODY_FIELDS``.

    Permissive pass-through for patch_types Plan 5 has not enumerated
    (the cross-lever router rejects unsupported patch_types later via
    the compatible-shape check)."""
    required = required_patch_body_fields(proposal.patch_type)
    missing = sorted(required - proposal.patch_body.keys())
    if missing:
        raise PatchBodyValidationError(
            f"patch_body for patch_type={proposal.patch_type.value!r} "
            f"missing required field(s): {missing}"
        )


def _validate_blame_set_in_identifier_allowlist(
    proposal: RepairProposal,
    *,
    identifier_allowlist: set[str],
) -> None:
    """Reject when any blame_set entry is not in the allowlist.

    Empty blame_set is vacuously valid (acceptable for prose patches
    like ADD_INSTRUCTION). Case-sensitive — UC identifiers in Genie
    Spaces are case-sensitive."""
    unknown = [b for b in proposal.blame_set if b not in identifier_allowlist]
    if unknown:
        raise ValueError(
            f"intent {proposal.intent_id!r}: blame_set entries outside "
            f"identifier_allowlist: {sorted(unknown)}"
        )


_LEAKAGE_NGRAM_SIZE = 5


def _normalize_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _ngrams(s: str, n: int) -> set[tuple[str, ...]]:
    tokens = _normalize_text(s).split()
    if len(tokens) < n:
        return set()
    return {tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1)}


def _validate_benchmark_leakage_relaxed_for_other(
    proposal: RepairProposal,
    *,
    benchmarks: list[dict[str, Any]] | None,
) -> None:
    """Relaxed n-gram firewall — only fires when ``repair_shape == OTHER``
    AND ``patch_type == ADD_EXAMPLE_SQL`` (the closed-gate bypass case).

    Closed-shape leakage gating runs INSIDE the L5b synthesis pipeline
    (synthesis.py's full firewall). This relaxed gate is the catch-all
    for the OTHER bypass — the cluster's repair_shape was outside the
    catalog, so the closed gate was skipped, and we still want a
    leakage check.

    No-op for non-OTHER shapes (closed gate handles them) and for
    non-ADD_EXAMPLE_SQL patch types (no example_question to check)."""
    if proposal.repair_shape is not RepairShape.OTHER:
        return
    if proposal.patch_type is not PatchType.ADD_EXAMPLE_SQL:
        return
    if not benchmarks:
        return

    question = str(proposal.patch_body.get("example_question") or "")
    if not question:
        return

    proposal_ngrams = _ngrams(question, _LEAKAGE_NGRAM_SIZE)
    if not proposal_ngrams:
        return

    for bm in benchmarks:
        bm_q = str((bm or {}).get("question") or "")
        if not bm_q:
            continue
        bm_ngrams = _ngrams(bm_q, _LEAKAGE_NGRAM_SIZE)
        if proposal_ngrams & bm_ngrams:
            overlap = next(iter(proposal_ngrams & bm_ngrams))
            raise ValueError(
                f"intent {proposal.intent_id!r}: relaxed leakage gate "
                f"rejected example_question — shares {_LEAKAGE_NGRAM_SIZE}-gram "
                f"{' '.join(overlap)!r} with a benchmark question"
            )


def _stamp_intent_id(*, cluster_id: str, ag_id: str, seq: int) -> str:
    """Build a deterministic intent_id.

    Format: ``intent_<cluster_id>_<ag_id>_<seq:03d>``.
    Mirrors the format Plan 1's ``intent_from_archetype`` uses (see
    ``repair_intent.py:292-294``) so postmortem can group intents from
    both producers under the same key.
    """
    if not cluster_id:
        raise ValueError("cluster_id must be non-empty")
    if not ag_id:
        raise ValueError("ag_id must be non-empty")
    if seq < 1:
        raise ValueError(f"seq must be ≥ 1; got {seq}")
    return f"intent_{cluster_id}_{ag_id}_{seq:03d}"
