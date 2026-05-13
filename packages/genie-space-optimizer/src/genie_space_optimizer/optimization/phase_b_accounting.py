"""Cycle 14-T1 — Phase B per-iter accounting helper.

Lifted out of harness.py:22905-22986 so producer exceptions earlier
in the iteration body cannot bypass the accounting block. Called
from ``_finalize_iteration_summary`` via the same try/finally
exit-path-total pattern Cycle 11 Bug B used for the invariant runner.

The helper is pure over the four accumulators in ``accumulators``
plus the four side-effect outputs (stdout marker, MLflow tag,
validator invocation, artifact-path tracking). Inputs are not
mutated.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def record_phase_b_iter_accounting(
    *,
    run_id: str,
    iteration: int,
    current_iter_inputs: dict[str, Any],
    journey_events: tuple[Any, ...] | list[Any] | None,
    producer_exceptions: dict[str, Any] | None,
    accumulators: dict[str, Any],
    contract_version: str,
) -> None:
    """Record one iteration's Phase B accounting.

    ``accumulators`` is mutated in place (this is the producer's only
    side-channel). Required keys:
      iter_record_counts, iter_violation_counts, no_records_iterations,
      total_violations (int), artifact_paths, _seen_iter_ids (set).

    Idempotent: if (run_id, iteration) has already been recorded, the
    function returns early. This makes the migration ramp safe even
    when both the legacy in-body block AND the new finalise-call site
    fire (e.g. during T1's flag-off ramp).
    """
    seen_id = (str(run_id), int(iteration))
    seen: set = accumulators.setdefault("_seen_iter_ids", set())
    if seen_id in seen:
        return
    seen.add(seen_id)

    from genie_space_optimizer.optimization.decision_emitters import (
        classify_no_records_reason,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord,
        validate_decisions_against_journey,
    )
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_no_records_marker,
    )

    iter_records_dicts = list(
        (current_iter_inputs or {}).get("decision_records") or []
    )
    iter_record_count = len(iter_records_dicts)
    accumulators.setdefault("iter_record_counts", []).append(iter_record_count)

    # Phase 3 T3.2.10 — count NEAR_MISS_AG_SHAPE_REPEATED reason codes
    # so the postmortem can see how often the AG-shape gate caught the
    # strategist proposing the same archetype/scope it tried before.
    try:
        _repeated_count = 0
        for _rec in iter_records_dicts:
            if not isinstance(_rec, dict):
                continue
            if str(_rec.get("reason_code") or "") == "near_miss_ag_shape_repeated":
                _repeated_count += 1
        if _repeated_count:
            accumulators["near_miss_reflection_repeated_ag_shape_count"] = (
                int(accumulators.get(
                    "near_miss_reflection_repeated_ag_shape_count", 0
                ) or 0) + _repeated_count
            )
    except Exception:
        logger.debug(
            "Phase B near-miss repeated-shape counter skipped",
            exc_info=True,
        )

    if iter_record_count == 0:
        no_rec_reason = classify_no_records_reason(
            iteration_inputs=current_iter_inputs or {},
            producer_exceptions=dict(producer_exceptions or {}),
        )
        accumulators.setdefault("no_records_iterations", []).append(int(iteration))
        accumulators.setdefault("iter_violation_counts", []).append(0)
        print(phase_b_no_records_marker(
            optimization_run_id=str(run_id),
            iteration=int(iteration),
            reason=no_rec_reason.value,
            producer_exceptions=dict(producer_exceptions or {}),
            contract_version=str(contract_version or ""),
        ))
        try:
            import mlflow as _mlflow
            if _mlflow.active_run() is not None:
                _mlflow.set_tags({
                    f"decision_trace.iter_{int(iteration)}.no_records_reason": no_rec_reason.value,
                    f"decision_trace.iter_{int(iteration)}.records": "0",
                })
        except Exception:
            logger.debug(
                "Phase B no-records MLflow tag skipped (non-fatal)",
                exc_info=True,
            )
        return

    accumulators.setdefault("artifact_paths", []).append(
        f"phase_b/decision_trace/iter_{int(iteration)}.json"
    )
    try:
        typed_records = [
            DecisionRecord.from_dict(r) for r in iter_records_dicts
        ]
        violations = validate_decisions_against_journey(
            records=typed_records,
            events=tuple(journey_events or ()),
        )
        violation_count = len(violations)
    except Exception:
        logger.debug(
            "Phase B per-iter validator skipped (non-fatal)",
            exc_info=True,
        )
        violation_count = 0
    accumulators.setdefault("iter_violation_counts", []).append(int(violation_count))
    accumulators["total_violations"] = (
        int(accumulators.get("total_violations", 0)) + int(violation_count)
    )
