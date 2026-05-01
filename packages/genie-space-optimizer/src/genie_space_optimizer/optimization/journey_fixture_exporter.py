"""Export real-run iteration inputs as a deterministic-replay fixture.

The Lever Loop's `_run_lever_loop` calls ``dump_replay_fixture`` at end-
of-run when ``GSO_DUMP_REPLAY_FIXTURE`` is set in the environment. The
output JSON matches the shape consumed by
``optimization.lever_loop_replay.run_replay`` (see
``tests/replay/fixtures/airline_5cluster.json`` for a worked example).

This is **inputs-only**, not events. The replay engine re-synthesizes
events from these inputs using its own deterministic emit logic, which
is decoupled from harness.py's `_journey_emit`. That decoupling is what
lets Phase D extract bits of harness.py without breaking the replay
test.
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
    return out


def dump_replay_fixture(
    *,
    path: str,
    fixture_id: str,
    iterations_data: list[dict[str, Any]],
) -> None:
    """Write a replay fixture JSON file from collected iteration inputs.

    Strips volatile fields (timestamps, durations, MLflow run IDs, any
    underscore-prefixed key) so the fixture is byte-stable across runs
    that produce identical inputs.

    ``iterations_data`` is the list captured by
    ``_collect_iteration_inputs`` inside ``_run_lever_loop`` — one dict
    per iteration with the keys listed in ``_ALLOWED_ITERATION_KEYS``.
    """
    fixture: dict[str, Any] = {
        "fixture_id": str(fixture_id),
        "iterations": [_strip_iteration(it) for it in (iterations_data or [])],
    }
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n")
