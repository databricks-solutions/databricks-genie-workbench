"""Typed eval row wrapper with canonical predicates.

The lever-loop emits eval rows in two shapes:

  * Production: ``{"question_id", "result_correctness" ∈ {"yes","no"},
    "arbiter" ∈ {"hard","soft","n/a","indeterminate"}, ...}``
  * Synthetic test fixtures: ``{"question_id", "row_status" ∈
    {"passing","hard","soft","unknown"}, ...}``

Both shapes have circulated long enough that helpers in the
codebase check inconsistent fields — the C14-V D-3 extension
(production ``accidentally_improved_qids: []``) was caused by
``compute_accidentally_improved_qids`` checking ``row_status`` only,
which is absent in production rows.

``EvalRow`` is the single canonical wrapper. Every helper that
classifies a row goes through it. Both shapes round-trip.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping


Classification = Literal["passing", "hard", "soft", "unknown"]


@dataclass(frozen=True, slots=True)
class EvalRow:
    """Frozen wrapper for one row of eval state."""

    question_id: str
    result_correctness: str = ""  # "yes" | "no" | ""
    arbiter: str = ""             # "hard" | "soft" | "n/a" | "indeterminate" | ""
    row_status: str = ""          # synthetic-only: "passing" | "hard" | "soft" | "unknown"
    extras: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "EvalRow":
        known = {"question_id", "result_correctness", "arbiter", "row_status"}
        extras = {k: v for k, v in raw.items() if k not in known}
        return cls(
            question_id=str(raw.get("question_id", "")),
            result_correctness=str(raw.get("result_correctness", "")),
            arbiter=str(raw.get("arbiter", "")),
            row_status=str(raw.get("row_status", "")),
            extras=dict(extras),
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"question_id": self.question_id}
        if self.result_correctness:
            out["result_correctness"] = self.result_correctness
        if self.arbiter:
            out["arbiter"] = self.arbiter
        if self.row_status:
            out["row_status"] = self.row_status
        out.update(self.extras)
        return out

    def is_passing(self) -> bool:
        """Canonical pass predicate.

        Truthy when EITHER production's ``result_correctness=yes``
        OR synthetic's ``row_status=passing``. Both shapes are
        accepted; a row that is silent on both is NOT passing.
        """
        if self.result_correctness.lower() == "yes":
            return True
        if self.row_status.lower() == "passing":
            return True
        return False

    def is_hard_failure(self) -> bool:
        if self.is_passing():
            return False
        if self.arbiter.lower() == "hard":
            return True
        if self.row_status.lower() == "hard":
            return True
        return False

    def is_soft_failure(self) -> bool:
        if self.is_passing():
            return False
        if self.arbiter.lower() == "soft":
            return True
        if self.row_status.lower() == "soft":
            return True
        return False

    def classification(self) -> Classification:
        if self.is_passing():
            return "passing"
        if self.is_hard_failure():
            return "hard"
        if self.is_soft_failure():
            return "soft"
        return "unknown"


def passing_qids(rows: list[EvalRow]) -> set[str]:
    return {r.question_id for r in rows if r.is_passing() and r.question_id}


def hard_failure_qids(rows: list[EvalRow]) -> set[str]:
    return {r.question_id for r in rows if r.is_hard_failure() and r.question_id}
