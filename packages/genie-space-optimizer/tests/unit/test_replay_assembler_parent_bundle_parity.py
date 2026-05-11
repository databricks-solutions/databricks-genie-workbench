"""RCO-1 — replay-assembler / parent-bundle parity guard.

The harness terminate path writes a fixed set of parent-level artifacts
(see ``bundle_artifact_paths`` in ``run_output_contract.py``). The
replay assembler must return every one of those keys that is materializable
from a replay fixture. Two parent-level paths are intentionally excluded:

- ``operator_transcript`` — a rendered Markdown artifact, not a JSON
  payload the assembler builds.
- ``replay_fixture`` — the assembler's own input; it does not echo it.

Every other parent-level key must be present in the assembler's return.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.run_output_bundle import (
    assemble_bundle_for_replay,
)
from genie_space_optimizer.optimization.run_output_contract import (
    bundle_artifact_paths,
)


# The two parent-level paths the replay assembler intentionally does not
# materialize. Update this set only when a deliberate plan adds or removes
# a parent-level artifact from the replay seam.
_REPLAY_EXCLUDED_PARENT_KEYS: frozenset[str] = frozenset({
    "operator_transcript",
    "replay_fixture",
})


def _canonical_parent_keys() -> set[str]:
    """Derive the canonical parent-level key set from the contract."""
    paths = bundle_artifact_paths(iterations=[1])
    return {k for k in paths.keys() if k != "iterations"}


def _minimal_replay_fixture() -> dict:
    """One-iteration fixture with the smallest valid shape the
    assembler accepts. Mirrors the production journey-validation
    report fields the harness terminate path reads."""
    return {
        "fixture_id": "rco1-parity-fixture",
        "baseline_accuracy": 50.0,
        "final_accuracy": 60.0,
        "delta_pp": 10.0,
        "iterations": [
            {
                "iteration": 1,
                "decision_records": [],
                "journey_violations": [],
                "bucket_assignments": {"sql_mismatch": ["q1", "q2"]},
                "stages": {},
            },
        ],
    }


def test_assembler_returns_every_canonical_parent_key() -> None:
    """Every parent-level key declared by ``bundle_artifact_paths``
    (minus the documented exclusions) must be a key of the assembler's
    return dict. A new parent artifact added without updating the
    assembler will fail this test."""
    expected = _canonical_parent_keys() - _REPLAY_EXCLUDED_PARENT_KEYS
    expected.add("iteration_summaries")

    result = assemble_bundle_for_replay(_minimal_replay_fixture())
    actual = set(result.keys())

    missing = expected - actual
    assert not missing, (
        f"Replay assembler missing parent-level keys: {sorted(missing)}. "
        f"Either wire the missing builder or add the key to "
        f"_REPLAY_EXCLUDED_PARENT_KEYS with a deliberate justification."
    )


def test_assembler_failure_buckets_payload_is_well_shaped() -> None:
    """The ``failure_buckets`` entry must be a dict produced by
    ``build_failure_buckets`` with the expected schema fields."""
    result = assemble_bundle_for_replay(_minimal_replay_fixture())

    fb = result["failure_buckets"]
    assert isinstance(fb, dict)
    assert fb.get("schema_version") == "v1"
    assert fb.get("iteration_count") == 1
    assert fb.get("total_failed_qid_events") == 2
    assert fb.get("bucket_counts") == {"sql_mismatch": 2}
    iterations = fb.get("iterations")
    assert isinstance(iterations, list) and len(iterations) == 1
    assert iterations[0]["iteration"] == 1
    assert iterations[0]["buckets"] == {"sql_mismatch": ["q1", "q2"]}
