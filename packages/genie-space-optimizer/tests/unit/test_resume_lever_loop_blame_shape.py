"""Pin the resume path's blame-normalization and tried-root-causes shape.

Bug 1 root cause: _build_reflection_entry writes blame as a tuple,
state.write_iteration JSON-encodes it, load_all_full_iterations JSON-
decodes it back to a list, and _resume_lever_loop then tries to add
the list to a set. This test simulates the JSON round-trip and verifies:

1. _resume_lever_loop does not raise TypeError on a list-shaped blame_set.
2. The restored tried_root_causes set contains the canonical tuple shape
   (root_cause, normalized_blame, frozenset(lever_set)) that
   _ag_collision_key produces on the live path.
3. _compute_forbidden_ag_set, when given a JSON-round-tripped reflection,
   produces the same forbidden tuple as the live path would.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pandas as pd

from genie_space_optimizer.optimization import harness
from genie_space_optimizer.optimization.harness import (
    _compute_forbidden_ag_set,
    _normalise_blame,
    _resume_lever_loop,
)
from genie_space_optimizer.optimization.rollback_class import RollbackClass


def _json_roundtripped_reflection() -> dict:
    """Simulate the persistence round-trip _build_reflection_entry would produce."""
    live = {
        "root_cause": "missing_filter",
        "blame_set": _normalise_blame(["fact.is_active", "time_window"]),
        "lever_set": [5, 6],
        "rollback_class": RollbackClass.CONTENT_REGRESSION.value,
        "accepted": False,
        "escalation_handled": False,
        "do_not_retry": [],
    }
    # state.write_iteration -> JSON -> load_all_full_iterations.
    return json.loads(json.dumps(live))


def test_resume_lever_loop_handles_json_decoded_blame_list() -> None:
    """The resume path must not crash on a JSON-decoded list-shaped blame_set."""
    decoded = _json_roundtripped_reflection()
    assert isinstance(decoded["blame_set"], list), (
        "JSON deserializes tuples to lists; this guard documents the contract."
    )

    with patch.object(harness, "load_latest_state_iteration", return_value={
        "iteration": 3,
        "model_id": "m-test",
        "overall_accuracy": 84.2,
        "scores_json": {},
    }), patch.object(harness, "load_stages", return_value=pd.DataFrame()), \
         patch.object(harness, "load_all_full_iterations", return_value=[
             {"reflection_json": decoded}
         ]):
        result = _resume_lever_loop(
            spark=None,  # unused in mocked path
            run_id="run-test",
            catalog="cat",
            schema="sch",
        )

    assert isinstance(result["tried_root_causes"], set)
    # Canonical tuple shape must be hashable; the set construction itself
    # is the implicit assertion. Exact membership is asserted below.
    assert len(result["tried_root_causes"]) >= 1, (
        "Restored tried_root_causes must include the rolled-back content-"
        "regression cluster from the persisted reflection."
    )
    # Every entry must be hashable; verify by re-hashing through frozenset.
    frozenset(result["tried_root_causes"])


def test_resume_lever_loop_canonicalizes_blame_to_match_live_collision_key() -> None:
    """Resume must canonicalize blame the same way _ag_collision_key does."""
    decoded = _json_roundtripped_reflection()

    with patch.object(harness, "load_latest_state_iteration", return_value={
        "iteration": 3,
        "model_id": "m-test",
        "overall_accuracy": 84.2,
        "scores_json": {},
    }), patch.object(harness, "load_stages", return_value=pd.DataFrame()), \
         patch.object(harness, "load_all_full_iterations", return_value=[
             {"reflection_json": decoded}
         ]):
        result = _resume_lever_loop(
            spark=None, run_id="run-test", catalog="cat", schema="sch",
        )

    canonical_blame = _normalise_blame(decoded["blame_set"])
    found = any(
        entry[0] == "missing_filter" and entry[1] == canonical_blame
        for entry in result["tried_root_causes"]
        if isinstance(entry, tuple) and len(entry) >= 2
    )
    assert found, (
        f"Expected a tried_root_causes entry whose blame matches the "
        f"canonical normalization {canonical_blame!r}; got "
        f"{result['tried_root_causes']!r}"
    )


def test_compute_forbidden_ag_set_normalizes_blame_for_resumed_buffer() -> None:
    """Forbidden-set keys must use _normalise_blame so resumed and live entries collide."""
    decoded = _json_roundtripped_reflection()

    forbidden = _compute_forbidden_ag_set([decoded])

    expected_blame = _normalise_blame(decoded["blame_set"])
    expected = ("missing_filter", expected_blame, frozenset({5, 6}))
    assert expected in forbidden, (
        f"_compute_forbidden_ag_set must normalize blame so JSON-round-"
        f"tripped reflections collide with live AGs. Expected {expected!r} "
        f"in {forbidden!r}"
    )
