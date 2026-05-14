"""Phase 0.4 — per-iteration candidate ledger.

Append-only JSONL artifact. One row per candidate attempt. Replaces
the prose ``reflection_buffer`` as the postmortem-friendly substrate
for iteration-level decisions.

Schema is **additive**: future Phase 1 fields (e.g., debt
classification, flags snapshot, timing) may be added without
bumping ``schema_version`` provided producers ALWAYS emit the
22 required fields listed in :data:`LEDGER_REQUIRED_FIELDS` and
consumers tolerate additional unknown fields.
"""
from __future__ import annotations

import fcntl
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


LEDGER_SCHEMA_VERSION: str = "v1"

LEDGER_REQUIRED_FIELDS: tuple[str, ...] = (
    "iteration",
    "ag_id",
    "cluster_ids",
    "target_qids",
    "root_cause",
    "requested_levers",
    "rca_card_id_or_provisional",
    "proposal_attempts",
    "selected_proposal_id",
    "terminal_reason",
    "terminal_outcome",
    "best_of_n_size",
    "patches_applied",
    "subset_isolation_run",
    "subset_isolation_kept",
    "subset_isolation_dropped",
    "protected_dependents",
    "narrow_replacement_attempted",
    "narrow_replacement_succeeded",
    "accuracy_delta_pp",
    "acceptance_tier",
    "retire_signature",
)


class LedgerSchemaError(Exception):
    """Raised when a ledger line fails schema validation."""


@dataclass(frozen=True)
class IterationCandidateLedgerEntry:
    """One row of ``iteration_candidate_ledger.jsonl``.

    Field names and types are CONTRACT — never reorder or rename;
    future versions may APPEND new optional fields under
    ``schema_version="v1"``.
    """

    iteration: int
    ag_id: str
    cluster_ids: tuple[str, ...]
    target_qids: tuple[str, ...]
    root_cause: str
    requested_levers: tuple[int, ...]
    rca_card_id_or_provisional: str
    proposal_attempts: int
    selected_proposal_id: str
    terminal_reason: str
    terminal_outcome: str
    best_of_n_size: int
    patches_applied: int
    subset_isolation_run: bool
    subset_isolation_kept: tuple[str, ...]
    subset_isolation_dropped: tuple[str, ...]
    protected_dependents: tuple[str, ...]
    narrow_replacement_attempted: bool
    narrow_replacement_succeeded: bool
    accuracy_delta_pp: float
    acceptance_tier: str
    retire_signature: str

    def to_jsonable(self) -> dict[str, Any]:
        """Return a JSON-serializable dict. Tuples become lists;
        ``schema_version`` is injected.
        """
        d: dict[str, Any] = {"schema_version": LEDGER_SCHEMA_VERSION}
        for k, v in asdict(self).items():
            d[k] = list(v) if isinstance(v, tuple) else v
        return d


def write_ledger_entry(
    entry: IterationCandidateLedgerEntry,
    *,
    path: str,
) -> None:
    """Append ``entry`` to the JSONL file at ``path``.

    Acquires an exclusive ``fcntl.LOCK_EX`` for the duration of the
    write so concurrent appends from sibling threads do not interleave
    lines. Creates parent directories as needed.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = entry.to_jsonable()
    line = json.dumps(payload, sort_keys=False) + "\n"
    with target.open("a", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        try:
            fh.write(line)
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def read_ledger(path: str) -> tuple[IterationCandidateLedgerEntry, ...]:
    """Parse every line of ``path`` into typed entries.

    Validates ``schema_version`` and the presence of every field in
    :data:`LEDGER_REQUIRED_FIELDS`. Raises :class:`LedgerSchemaError`
    on any violation.
    """
    out: list[IterationCandidateLedgerEntry] = []
    text = Path(path).read_text(encoding="utf-8")
    for line_no, raw in enumerate(text.splitlines(), start=1):
        stripped = raw.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise LedgerSchemaError(
                f"line {line_no}: invalid JSON: {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise LedgerSchemaError(
                f"line {line_no}: payload not a JSON object"
            )
        version = payload.get("schema_version")
        if version != LEDGER_SCHEMA_VERSION:
            raise LedgerSchemaError(
                f"line {line_no}: unknown schema_version={version!r} "
                f"(expected {LEDGER_SCHEMA_VERSION!r})"
            )
        missing = [f for f in LEDGER_REQUIRED_FIELDS if f not in payload]
        if missing:
            raise LedgerSchemaError(
                f"line {line_no}: missing required fields: {missing}"
            )
        out.append(IterationCandidateLedgerEntry(
            iteration=int(payload["iteration"]),
            ag_id=str(payload["ag_id"]),
            cluster_ids=tuple(str(c) for c in payload["cluster_ids"]),
            target_qids=tuple(str(q) for q in payload["target_qids"]),
            root_cause=str(payload["root_cause"]),
            requested_levers=tuple(int(L) for L in payload["requested_levers"]),
            rca_card_id_or_provisional=str(payload["rca_card_id_or_provisional"]),
            proposal_attempts=int(payload["proposal_attempts"]),
            selected_proposal_id=str(payload["selected_proposal_id"]),
            terminal_reason=str(payload["terminal_reason"]),
            terminal_outcome=str(payload["terminal_outcome"]),
            best_of_n_size=int(payload["best_of_n_size"]),
            patches_applied=int(payload["patches_applied"]),
            subset_isolation_run=bool(payload["subset_isolation_run"]),
            subset_isolation_kept=tuple(
                str(p) for p in payload["subset_isolation_kept"]
            ),
            subset_isolation_dropped=tuple(
                str(p) for p in payload["subset_isolation_dropped"]
            ),
            protected_dependents=tuple(
                str(q) for q in payload["protected_dependents"]
            ),
            narrow_replacement_attempted=bool(
                payload["narrow_replacement_attempted"]
            ),
            narrow_replacement_succeeded=bool(
                payload["narrow_replacement_succeeded"]
            ),
            accuracy_delta_pp=float(payload["accuracy_delta_pp"]),
            acceptance_tier=str(payload["acceptance_tier"]),
            retire_signature=str(payload["retire_signature"]),
        ))
    return tuple(out)


def emit_ledger_marker(
    entry: IterationCandidateLedgerEntry,
    *,
    optimization_run_id: str,
) -> str:
    """Return the stdout marker line for ``entry``.

    Wrapper around
    :func:`genie_space_optimizer.optimization.run_analysis_contract.candidate_ledger_entry_marker`
    so harness call sites can import a single name from
    ``candidate_ledger``.
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        candidate_ledger_entry_marker,
    )

    return candidate_ledger_entry_marker(
        optimization_run_id=optimization_run_id,
        entry=entry.to_jsonable(),
    )
