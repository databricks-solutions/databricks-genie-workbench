"""Phase 4 — production-shape replay through both authoritative consumers.

This is the canary that should have existed before each of the last five
redesigns. It loads MLflow-shaped eval rows (qid under ``inputs/question_id``
etc.) and exercises both authoritative admission paths end-to-end:

* ``cluster_failures(...)`` — drives Plan 11 dispatch via
  ``_stamp_failing_qids_from_eval_results``. The
  ``GSO_PLAN11_DISPATCH_DECISION_V1`` marker must show
  ``failing_qids_count>0`` and the expected hard qids.
* ``run_state_machine_iteration_and_persist(...)`` — drives the v4 state
  machine through ``build_initial_states_from_eval_rows`` and must return
  one ``QuestionStateInIteration`` per hard row.

Pre-Phase-2, both calls would silently return zero hard qids because the
admission adapters used ``row.get("question_id")``.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "production_eval_rows.json"
)


@pytest.fixture(scope="module")
def production_rows() -> list[dict]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [dict(r) for r in data["eval_rows"]]


@pytest.fixture(scope="module")
def expected_hard_qids() -> list[str]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [
        r["_expected_qid"]
        for r in data["eval_rows"]
        if r.get("_expected_hard") is True
    ]


def _find_marker(captured: str, marker: str) -> dict | None:
    for line in captured.splitlines():
        if line.startswith(marker + " "):
            try:
                return json.loads(line.split(" ", 1)[1])
            except json.JSONDecodeError:
                continue
    return None


def test_cluster_failures_stamps_production_shape_hard_qids(
    production_rows: list[dict],
    expected_hard_qids: list[str],
) -> None:
    """``cluster_failures`` must stamp the canonical hard qids into the
    metadata snapshot regardless of whether the rows carry the qid as
    ``question_id``, ``inputs/question_id``, or ``request.kwargs.question_id``.
    """
    from genie_space_optimizer.optimization.optimizer import cluster_failures

    eval_results = {"rows": production_rows}
    metadata_snapshot: dict = {}

    # cluster_failures performs the stamping side-effect and returns clusters.
    # We don't care about the clusters here; the test is about the
    # admission stamper running with production-shape rows.
    _ = cluster_failures(
        eval_results,
        metadata_snapshot,
        spark=None,
        run_id="test_phase4",
        catalog="",
        schema="",
        signal_type="hard",
        namespace="H",
        verbose=False,
    )
    stamped = metadata_snapshot.get("_failing_qids") or []
    assert sorted(stamped) == sorted(expected_hard_qids)


def test_run_state_machine_iteration_returns_states_for_production_shape_rows(
    production_rows: list[dict],
    expected_hard_qids: list[str],
    tmp_path: Path,
) -> None:
    """``run_state_machine_iteration_and_persist`` must build one
    ``QuestionStateInIteration`` per hard production-shape row. The SM may
    decline to transform any further (no workspace client, no real LLM), but
    the initial states must be present — that is what was silently empty
    before Phase 2.
    """
    from genie_space_optimizer.optimization.optimizer import (
        run_state_machine_iteration_and_persist,
    )

    final_states = run_state_machine_iteration_and_persist(
        eval_rows=production_rows,
        iteration=1,
        run_id="test_phase4",
        run_root=tmp_path,
        workspace_client=None,
        forbidden_signatures=(),
    )

    qids = sorted(str(s.qid) for s in final_states)
    assert qids == sorted(expected_hard_qids)
