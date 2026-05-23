"""Fail-loud parity contract between the harness and the authoritative
admission consumers (Plan 11 dispatch and the v4 state machine).

Five architectural redesigns in a row produced zero accuracy improvement
because the harness saw N>0 hard rows while the authoritative consumers
silently received zero — each hand-rolled a different ``row.get(...)``
ladder, and no boundary contract asserted parity.

This module exposes:

* :func:`assert_input_projection_parity` — call once per iteration, after
  Plan 11 dispatch returns and the SM ``run_state_machine_iteration_and_persist``
  call returns. Emits ``GSO_INPUT_PROJECTION_PARITY_V1`` so drift between the
  three sets is observable, and raises
  :class:`InputProjectionContractViolation` if the harness saw hard rows but
  BOTH authoritative consumers received zero.
* :class:`InputProjectionContractViolation` — a typed ``RuntimeError`` subclass
  the lever-loop's broad ``except Exception`` MUST be narrowed against so it
  propagates and aborts the run instead of degrading silently to the legacy
  lane.
"""
from __future__ import annotations

from collections.abc import Iterable

from genie_space_optimizer.optimization.state_machine.markers import (
    input_projection_contract_violation_marker,
    input_projection_parity_marker,
)


class InputProjectionContractViolation(RuntimeError):
    """Raised when authoritative admission consumers starve on hard rows.

    The harness saw N>0 hard rows but BOTH Plan 11 dispatch AND the state
    machine entry adapter received zero. This indicates a row-shape
    mismatch at the admission boundary and the run must abort rather than
    fall back silently to the legacy lane.
    """


def assert_input_projection_parity(
    *,
    iteration: int,
    harness_hard_qids: Iterable[str],
    plan11_hard_qids: Iterable[str],
    state_machine_hard_qids: Iterable[str],
) -> None:
    """Emit the per-iteration parity marker and raise on full starvation.

    ``harness_hard_qids`` is the authoritative reference set computed by the
    harness via ``row_is_hard_failure`` + ``extract_question_id`` over the
    same eval rows it hands to both consumers. ``plan11_hard_qids`` and
    ``state_machine_hard_qids`` are what each authoritative consumer
    reported. Empty harness set is a no-op (nothing to assert).
    """
    harness_set = {str(q) for q in harness_hard_qids if q}
    if not harness_set:
        return

    plan11_set = {str(q) for q in plan11_hard_qids if q}
    sm_set = {str(q) for q in state_machine_hard_qids if q}

    missing_from_plan11 = sorted(harness_set - plan11_set)
    missing_from_sm = sorted(harness_set - sm_set)

    print(
        input_projection_parity_marker(
            iteration=iteration,
            harness_hard_qids=list(harness_set),
            plan11_hard_qids=list(plan11_set),
            state_machine_hard_qids=list(sm_set),
            missing_from_plan11=missing_from_plan11,
            missing_from_sm=missing_from_sm,
        ),
        flush=True,
    )

    # Fail closed only on total starvation: BOTH authoritative consumers
    # received zero while the harness has hard rows. Partial drift is
    # observable via the parity marker but is not fatal — a single
    # missing qid can be a legitimate downstream filter (e.g. quarantine).
    if not plan11_set and not sm_set:
        print(
            input_projection_contract_violation_marker(
                iteration=iteration,
                harness_hard_count=len(harness_set),
                plan11_hard_count=0,
                sm_hard_count=0,
            ),
            flush=True,
        )
        raise InputProjectionContractViolation(
            f"Authoritative admission starved at iteration={iteration}: "
            f"harness saw {len(harness_set)} hard qids "
            f"({sorted(harness_set)}) but Plan 11 dispatch and the state "
            f"machine both received zero. This is the row-shape mismatch "
            f"that the canonical extractor at "
            f"genie_space_optimizer.optimization._qid_extraction.extract_question_id "
            f"exists to prevent; one or more dispatch callsites is "
            f"bypassing it."
        )
