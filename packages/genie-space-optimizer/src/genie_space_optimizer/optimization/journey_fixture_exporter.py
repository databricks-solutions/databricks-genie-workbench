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
    # L5-dispatch replay (2026-05-16) — four new keys captured just
    # before the forced-synthesis dispatch fires. These reconstruct the
    # exact inputs dispatch_forced_structural_synthesis reads, so an
    # offline replay can reproduce dispatch decisions byte-for-byte.
    "lever5_gate_drops",
    "iter_source_clusters_by_id",
    "metadata_failure_clusters",
    "iter_rca_id_by_cluster",
)


_ALLOWED_L5_DROP_KEYS = (
    "ag_id",
    "source_clusters",
    "root_causes",
    "target_lever",
    "had_example_sqls",
    "instruction_sections_dropped",
    "instruction_guidance_dropped",
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
    # L5-dispatch replay (2026-05-16) — preserve the asi_failure_type
    # label that the structural gate stores into
    # _LEVER5_GATE_DROPS[*].root_causes. Without this key, the replay
    # cannot reproduce the label-divergence dispatch path.
    "asi_failure_type",
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


def _coerce_record_to_dict(record: Any) -> dict | None:
    """B5 (2026-05-13) — coerce a decision-record entry to a plain dict.

    Mirrors ``invariant_projection._record_to_mapping`` (B2). Branches:

    1. Already a Mapping → ``dict(record)``.
    2. Exposes a callable ``to_dict()`` (the ``DecisionRecord`` contract at
       ``rca_decision_trace.py:476``) → invoke and coerce; if ``to_dict()``
       raises or returns a non-Mapping, return ``None`` so the caller drops
       the record.
    3. Anything else → return ``None``.

    Closes the 2314bb2c empty-fixture failure where a ``DecisionRecord``
    dataclass in ``iterations_data[i]["decision_records"]`` made
    ``_strip_dict``'s ``k in d`` check raise ``TypeError: argument of
    type 'DecisionRecord' is not iterable``, which the harness's outer
    try-except swallowed silently and produced an empty
    ``replay_fixture.json``.

    Evidence anchor:
    docs/runid_analysis/2314bb2c-95a1-4d60-8226-09e5155aee2a/postmortem.md F8
    """
    from collections.abc import Mapping as _Mapping

    if isinstance(record, _Mapping):
        return dict(record)
    to_dict_attr = getattr(record, "to_dict", None)
    if callable(to_dict_attr):
        try:
            result = to_dict_attr()
        except Exception:
            return None
        if isinstance(result, _Mapping):
            return dict(result)
        return None
    return None


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
        # B5 (2026-05-13) — coerce each record to a dict before _strip_dict.
        # Drops non-coercible entries (strings, ints, dataclasses without
        # to_dict). Without this, a stray DecisionRecord dataclass made
        # ``k in d`` raise TypeError and the outer try-except swallowed
        # the whole iteration's serialization.
        _coerced: list[dict] = []
        for r in (out.get("decision_records") or []):
            d = _coerce_record_to_dict(r)
            if d is not None:
                _coerced.append(_strip_dict(d, _ALLOWED_DECISION_RECORD_KEYS))
        out["decision_records"] = _coerced
    if "lever5_gate_drops" in out:
        out["lever5_gate_drops"] = [
            _strip_dict(d, _ALLOWED_L5_DROP_KEYS)
            for d in (out.get("lever5_gate_drops") or [])
            if isinstance(d, dict)
        ]
    if "iter_source_clusters_by_id" in out:
        # Dict keyed by cluster_id; each value is a small cluster dict
        # with the same shape as ``clusters[*]`` (incl. asi_failure_type).
        src = out.get("iter_source_clusters_by_id") or {}
        out["iter_source_clusters_by_id"] = {
            str(k): _strip_dict(v, _ALLOWED_CLUSTER_KEYS)
            for k, v in src.items()
            if isinstance(v, dict)
        }
    if "iter_rca_id_by_cluster" in out:
        # Dict[str, str].
        src = out.get("iter_rca_id_by_cluster") or {}
        out["iter_rca_id_by_cluster"] = {
            str(k): str(v) for k, v in src.items() if v is not None
        }
    if "metadata_failure_clusters" in out:
        # List of cluster dicts shaped like the clusters key. The full
        # ``metadata_snapshot._failure_clusters`` payload can be large;
        # we keep only the fields the dispatch reads.
        out["metadata_failure_clusters"] = [
            _strip_dict(c, _ALLOWED_CLUSTER_KEYS)
            for c in (out.get("metadata_failure_clusters") or [])
            if isinstance(c, dict)
        ]
    return out


def _build_fixture(
    *,
    fixture_id: str,
    iterations_data: list[dict[str, Any]],
) -> dict[str, Any]:
    """B5 (2026-05-13) — per-iteration resilience.

    One malformed iteration must not bring down the whole serialization.
    The healthy path is unchanged: every iteration's strip output is
    appended in order. The failure path: a strip raise is caught,
    logged, and the iteration is skipped — the rest still serialize.
    """
    import logging as _logging
    _stripped: list[dict[str, Any]] = []
    for it in (iterations_data or []):
        try:
            _stripped.append(_strip_iteration(it))
        except Exception:
            _logging.getLogger(__name__).warning(
                "Phase A: skipping malformed iteration during strip "
                "(non-fatal)",
                exc_info=True,
            )
    return {
        "fixture_id": str(fixture_id),
        "iterations": _stripped,
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
