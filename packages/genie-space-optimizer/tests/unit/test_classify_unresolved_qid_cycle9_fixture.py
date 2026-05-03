"""Phase D failure-bucketing T6: cycle-9 fixture sanity test (skipif-guarded).

If the cycle-9 raw replay fixture carries iteration-level decision
records, every unresolved qid should classify to a non-None bucket.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


_FIXTURE_PATH = (
    Path(__file__).parent.parent / "replay" / "fixtures"
    / "airline_real_v1_cycle9_raw.json"
)


def _load_iter1_records():
    if not _FIXTURE_PATH.exists():
        return None
    with _FIXTURE_PATH.open() as f:
        payload = json.load(f)
    iterations = payload.get("iterations") or []
    if not iterations:
        return None
    iter1 = iterations[0] if isinstance(iterations, list) else None
    if not isinstance(iter1, dict):
        return None
    return iter1.get("decision_records") or None


@pytest.mark.skipif(
    _load_iter1_records() is None,
    reason="cycle-9 raw fixture lacks decision_records",
)
def test_every_cycle9_unresolved_qid_has_a_bucket_label():
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, OptimizationTrace,
    )

    rows = _load_iter1_records()
    records = tuple(DecisionRecord.from_dict(row) for row in rows or [])
    trace = OptimizationTrace(decision_records=records)
    unresolved_qids = {
        r.question_id for r in records
        if r.decision_type == DecisionType.QID_RESOLUTION
        and r.outcome == DecisionOutcome.UNRESOLVED
        and r.question_id
    }
    if not unresolved_qids:
        pytest.skip("cycle-9 iter 1 had no unresolved qids")
    valid_buckets = set(FailureBucket)
    for qid in unresolved_qids:
        result = classify_unresolved_qid(trace, qid, iteration=1)
        assert result.bucket in valid_buckets, (
            f"qid {qid} classified to None despite being unresolved"
        )
