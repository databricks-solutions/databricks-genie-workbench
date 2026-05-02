"""Export real-run iteration inputs as a deterministic-replay fixture.

The Lever Loop's ``_run_lever_loop`` always emits the fixture at end-of-
run via two channels:
  1. ``serialize_replay_fixture(...)`` returns a compact single-line
     JSON string, which the harness prints to stderr between
     ``===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===`` and
     ``===PHASE_A_REPLAY_FIXTURE_JSON_END===`` markers. The user
     extracts the fixture from job logs.
  2. When an MLflow run is active, the harness also calls
     ``mlflow.log_dict(...)`` so the fixture is downloadable from the
     MLflow UI without any log-grep work.

The output JSON matches the shape consumed by
``optimization.lever_loop_replay.run_replay`` (see
``tests/replay/fixtures/airline_5cluster.json`` for a worked example).
This is **inputs-only**, not events. The replay engine re-synthesizes
events from these inputs using its own deterministic emit logic, which
is decoupled from harness.py's ``_journey_emit``. That decoupling is
what lets Phase D extract bits of harness.py without breaking the
replay test.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_ALLOWED_TOP_KEYS = ("fixture_id", "iterations")
_ALLOWED_ITERATION_KEYS = (
    "iteration",
    "eval_rows",
    "clusters",
    "soft_clusters",
    "strategist_response",
    "ag_outcomes",
    "post_eval_passing_qids",
    "journey_validation",
    "decision_records",
)
_ALLOWED_EVAL_ROW_KEYS = (
    "question_id",
    "result_correctness",
    "arbiter",
)
_ALLOWED_CLUSTER_KEYS = (
    "cluster_id",
    "root_cause",
    "question_ids",
)
_ALLOWED_AG_KEYS = (
    "id",
    "affected_questions",
    "patches",
)
_ALLOWED_PATCH_KEYS = (
    "proposal_id",
    "patch_type",
    "target_qids",
    "cluster_id",
)
_ALLOWED_DECISION_RECORD_KEYS = (
    "run_id",
    "iteration",
    "decision_type",
    "outcome",
    "reason_code",
    "question_id",
    "cluster_id",
    "rca_id",
    "root_cause",
    "ag_id",
    "proposal_id",
    "patch_id",
    "gate",
    "reason_detail",
    "affected_qids",
    "evidence_refs",
    "target_qids",
    "expected_effect",
    "observed_effect",
    "regression_qids",
    "next_action",
    "source_cluster_ids",
    "proposal_ids",
    "metrics",
)


def _strip_dict(d: dict, allowed: tuple[str, ...]) -> dict:
    return {k: d[k] for k in allowed if k in d}


def _strip_iteration(it: dict[str, Any]) -> dict[str, Any]:
    out = _strip_dict(it, _ALLOWED_ITERATION_KEYS)
    if "eval_rows" in out:
        out["eval_rows"] = [
            _strip_dict(r, _ALLOWED_EVAL_ROW_KEYS)
            for r in (out.get("eval_rows") or [])
        ]
    if "clusters" in out:
        out["clusters"] = [
            _strip_dict(c, _ALLOWED_CLUSTER_KEYS)
            for c in (out.get("clusters") or [])
        ]
    if "soft_clusters" in out:
        out["soft_clusters"] = [
            _strip_dict(c, _ALLOWED_CLUSTER_KEYS)
            for c in (out.get("soft_clusters") or [])
        ]
    if "strategist_response" in out:
        sr = out["strategist_response"] or {}
        out["strategist_response"] = {
            "action_groups": [
                {
                    **_strip_dict(ag, _ALLOWED_AG_KEYS),
                    "patches": [
                        _strip_dict(p, _ALLOWED_PATCH_KEYS)
                        for p in (ag.get("patches") or [])
                    ],
                }
                for ag in (sr.get("action_groups") or [])
            ],
        }
    if "decision_records" in out:
        out["decision_records"] = [
            _strip_dict(r, _ALLOWED_DECISION_RECORD_KEYS)
            for r in (out.get("decision_records") or [])
        ]
    return out


def _build_fixture(
    *,
    fixture_id: str,
    iterations_data: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "fixture_id": str(fixture_id),
        "iterations": [_strip_iteration(it) for it in (iterations_data or [])],
    }


def serialize_replay_fixture(
    *,
    fixture_id: str,
    iterations_data: list[dict[str, Any]],
) -> str:
    """Return a compact single-line JSON serialization of the fixture.

    This is the primary runtime API. The single-line shape is what the
    user's log-extractor script (and any ad-hoc grep) relies on.
    """
    fixture = _build_fixture(
        fixture_id=fixture_id, iterations_data=iterations_data,
    )
    return json.dumps(fixture, sort_keys=True, separators=(",", ":"))


def dump_replay_fixture(
    *,
    path: str,
    fixture_id: str,
    iterations_data: list[dict[str, Any]],
) -> None:
    """Write a pretty-printed replay fixture JSON file.

    Used by unit tests. Not called at runtime — the harness uses
    ``serialize_replay_fixture`` and emits via stderr + MLflow.
    """
    fixture = _build_fixture(
        fixture_id=fixture_id, iterations_data=iterations_data,
    )
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n")


def begin_iteration_capture(
    *,
    iterations_data: list[dict[str, Any]],
    iteration: int,
) -> dict[str, Any]:
    """Allocate a fresh iteration snapshot, append it, and return its ref.

    Append-on-begin is the contract: the snapshot enters the run-level
    ``iterations_data`` list immediately, before any code path can
    ``continue`` or ``break`` past a late-append site. The returned dict
    is the exact reference appended, so subsequent in-place mutation of
    its ``eval_rows``, ``clusters``, ``soft_clusters``,
    ``strategist_response``, ``ag_outcomes``, and
    ``post_eval_passing_qids`` keys is automatically reflected in the
    list. This is what makes rollback paths, cap drops, and diagnostic
    AG paths unable to silently drop an iteration from the replay
    fixture.
    """
    snapshot: dict[str, Any] = {
        "iteration": int(iteration),
        "eval_rows": [],
        "clusters": [],
        "soft_clusters": [],
        "strategist_response": {"action_groups": []},
        "ag_outcomes": {},
        "post_eval_passing_qids": [],
        "journey_validation": None,
        "decision_records": [],
    }
    iterations_data.append(snapshot)
    return snapshot


def summarize_replay_fixture(
    *,
    iterations_data: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return a compact summary of the replay fixture for log emission.

    Operators use this to validate a real run's fixture without parsing
    the JSON body — if ``iterations`` is 0 or any iteration's
    ``eval_rows`` is 0, fixture capture failed and the run should be
    triaged before extraction.
    """
    per_iter: list[dict[str, int]] = []
    for it in iterations_data or []:
        sr = (it.get("strategist_response") or {})
        per_iter.append(
            {
                "iteration": int(it.get("iteration") or 0),
                "eval_rows": len(it.get("eval_rows") or []),
                "clusters": len(it.get("clusters") or []),
                "soft_clusters": len(it.get("soft_clusters") or []),
                "action_groups": len(sr.get("action_groups") or []),
                "ag_outcomes": len(it.get("ag_outcomes") or {}),
                "post_eval_passing_qids": len(
                    it.get("post_eval_passing_qids") or []
                ),
                "decision_records": len(it.get("decision_records") or []),
            }
        )
    return {
        "iterations": len(iterations_data or []),
        "per_iter": per_iter,
    }
