"""Phase 2 Action 2.3 — Proactive scoped variants for hub-table patches.

When a proposal targets a hub table (passing_dependents > threshold),
the proposal pipeline emits a per-qid scoped sibling. The two siblings
flow through the gate independently:
* Original — broad, possibly dropped by blast-radius gate.
* Scoped variant — narrow, often surviving where the original is dropped.

The Section B kit-safety gate downgrades a kit's risk class from
``high`` to ``medium`` when a scoped alternative is available.

All functions in this module are pure (no I/O, no logger).
"""

from __future__ import annotations

from typing import Iterable


def is_hub_table_patch(patch: dict, *, threshold: int) -> bool:
    """Return True iff ``patch`` targets a hub table.

    Definition: target_table is set AND passing_dependents count
    exceeds ``threshold``.
    """
    target_table = str(patch.get("target_table") or "").strip()
    if not target_table:
        return False
    raw = patch.get("passing_dependents") or []
    return len(list(raw)) > int(threshold)


def scoped_alternative_available(
    patches: Iterable[dict], *, target_pid: str,
) -> bool:
    """Return True iff any patch in ``patches`` carries
    ``_scoped_from_pid == target_pid``."""
    target = str(target_pid).strip()
    if not target:
        return False
    return any(
        str(p.get("_scoped_from_pid") or "") == target for p in patches
    )


def generate_scoped_variant(
    patch: dict,
    *,
    target_qids: tuple[str, ...],
    threshold: int = 2,
) -> dict | None:
    """Return a scoped sibling of ``patch`` whose footprint is limited
    to ``target_qids``, or ``None`` when:
    * ``patch`` is not a hub-table patch (per :func:`is_hub_table_patch`).
    * ``target_qids`` is empty.

    The sibling has:
    * a new ``proposal_id`` (parent's id + ``_scoped`` suffix),
    * ``_scoped_from_pid`` pointing back to the parent,
    * ``scoped_to_qids`` set to the target qids,
    * ``passing_dependents`` reduced to the intersection of the
      parent's dependents and the target qids.

    The default threshold is permissive (2) so any hub-table patch with
    3+ dependents is eligible for a scoped sibling at generation time;
    the production gate threshold (default 5, tunable via
    ``hub_table_dependents_threshold``) decides whether to *emit* the
    variant. Generation and emission are decoupled deliberately so the
    sibling can be precomputed without coupling to runtime config.
    """
    if not target_qids:
        return None
    if not is_hub_table_patch(patch, threshold=threshold):
        return None

    parent_pid = str(patch.get("proposal_id") or patch.get("id") or "")
    if not parent_pid:
        return None

    target_set = {str(q) for q in target_qids if str(q)}
    original_dependents = [str(q) for q in (patch.get("passing_dependents") or []) if str(q)]
    scoped_dependents = [q for q in original_dependents if q in target_set]

    variant: dict = dict(patch)
    variant["proposal_id"] = f"{parent_pid}_scoped"
    variant["_scoped_from_pid"] = parent_pid
    variant["scoped_to_qids"] = tuple(sorted(target_set))
    variant["passing_dependents"] = scoped_dependents
    return variant
