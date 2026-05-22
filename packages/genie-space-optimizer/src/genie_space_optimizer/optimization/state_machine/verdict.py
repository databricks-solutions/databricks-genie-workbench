"""GateVerdict and TransformerContext."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Union

from genie_space_optimizer.optimization.state_machine.records import (
    ProposalAttempt,
    TerminalRecord,
)


RejectionOutcome = Union[ProposalAttempt, TerminalRecord]


@dataclass(frozen=True, slots=True)
class GateVerdict:
    passed: bool
    success_record: Any | None = None
    rejection_outcome: RejectionOutcome | None = None

    def __post_init__(self):
        if self.passed and self.rejection_outcome is not None:
            raise ValueError("GateVerdict cannot be both passed and rejected")
        if not self.passed and self.rejection_outcome is None:
            raise ValueError("non-passing GateVerdict requires a rejection_outcome")

    @classmethod
    def success(cls, *, record: Any | None = None) -> "GateVerdict":
        return cls(passed=True, success_record=record, rejection_outcome=None)

    @classmethod
    def reject_terminal(cls, terminal: TerminalRecord) -> "GateVerdict":
        return cls(passed=False, rejection_outcome=terminal)

    @classmethod
    def reject_proposal(cls, attempt: ProposalAttempt) -> "GateVerdict":
        return cls(passed=False, rejection_outcome=attempt)


@dataclass(frozen=True, slots=True)
class ValidationContext:
    """Per-iteration context passed to ValidationGate predicates."""
    iteration: int
    run_id: str
    extras: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class TransformerContext:
    """Per-iteration context passed to every transformer."""
    iteration: int
    run_id: str
    validation_context: ValidationContext
    forbidden_signatures: tuple[str, ...] = ()
    extras: Mapping[str, Any] = field(default_factory=dict)
