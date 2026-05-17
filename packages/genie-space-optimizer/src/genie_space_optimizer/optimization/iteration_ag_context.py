"""Phase 1 (2026-05-16) — per-iteration AG-context capture helper.

The harness lever-loop declares five iter-locals at iter top
(``_iter_ag_id_for_ledger`` and friends; see ``harness.py:18591-18595``)
so the ``finally:`` block can always build the
``IterationCandidateLedgerEntry`` regardless of where the iteration
exited. Three of those locals
(``_iter_target_qids_for_ledger``,
``_iter_levers_for_ledger``, ``_iter_root_cause_for_ledger``) are
never assigned anywhere in the harness today — Phase 0.4 Task 13
introduced them but the wiring step was never landed (Bug 1).
``_iter_ag_id_for_ledger`` and ``_iter_cluster_ids_for_ledger`` ARE
assigned, but only at the per-AG terminal-emit sites; the full-eval
path emits its terminal marker first so the late capture at
``harness.py:29689`` is unreachable for that path — Bug 4.

This helper produces all five iter-local values from one call on the
strategist-emitted AG dict. The harness calls it once per AG, right
after ``ag_id`` is computed at ``harness.py:21869``, BEFORE any
terminal-emit predicate can fire.
"""

from __future__ import annotations

from typing import Any, Mapping


def capture_iter_ag_context(*, ag: Mapping[str, Any], ag_id: str) -> dict:
    """Snapshot the AG's identity fields for the candidate-ledger row.

    Returns a dict whose keys match the iter-locals the harness reads
    when building ``IterationCandidateLedgerEntry``. Pure — no I/O,
    no globals, no env reads.

    Field semantics:

    * ``ag_id`` — caller's value wins; falls back to ``ag["id"]`` /
      ``ag["ag_id"]``. Empty string when both are missing.
    * ``cluster_ids`` — tuple of non-empty stringified
      ``ag["source_cluster_ids"]`` entries. Numeric ids coerced.
    * ``target_qids`` — tuple of non-empty stringified
      ``ag["target_qids"]`` entries; falls back to
      ``ag["affected_questions"]``.
    * ``levers`` — sorted tuple of integer
      ``ag["lever_directives"]`` keys. Keys are typically strings.
    * ``root_cause`` — ``ag["root_cause"]`` or
      ``ag["root_cause_summary"]``.
    * ``blame_set`` — canonically-sorted tuple of stringified
      ``ag["blame_set"]`` (or ``ag["blamed_assets"]``) entries. Empty
      tuple when neither field is present. Phase 2 (2026-05-16) wires
      this into ``TerminalSignature.blame_set_norm``.
    """
    raw_cluster_ids = ag.get("source_cluster_ids") or ()
    cluster_ids = tuple(
        str(c) for c in raw_cluster_ids if c is not None and str(c)
    )

    raw_target = ag.get("target_qids") or ag.get("affected_questions") or ()
    target_qids = tuple(
        str(q) for q in raw_target if q is not None and str(q)
    )

    levers_dict = ag.get("lever_directives") or {}
    levers: tuple[int, ...] = tuple(
        sorted(int(k) for k in levers_dict.keys() if str(k).isdigit())
    )

    root_cause = str(
        ag.get("root_cause") or ag.get("root_cause_summary") or ""
    )

    resolved_ag_id = str(
        ag_id or ag.get("id") or ag.get("ag_id") or ""
    )

    raw_blame = ag.get("blame_set") or ag.get("blamed_assets") or ()
    blame_set = tuple(sorted(
        str(b).strip() for b in raw_blame
        if b is not None and str(b).strip()
    ))

    return {
        "ag_id": resolved_ag_id,
        "cluster_ids": cluster_ids,
        "target_qids": target_qids,
        "levers": levers,
        "root_cause": root_cause,
        "blame_set": blame_set,
    }
