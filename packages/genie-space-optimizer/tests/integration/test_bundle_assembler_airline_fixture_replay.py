"""Cycle 14-W T2 — bundle assembler must not raise ``AttributeError``
on list-valued stage captures.

Anchor: airline run 1105451933925748 F7 (run dir
``1099b152-8655-4f1e-ab43-1240a9400280``).

Discipline A: this is a regressed defect (D-4) — unit test alone
is insufficient. We replay the actual production fixture if it is
vendored; otherwise the test is skipped (the unit-level coverage
in ``test_bundle_assembler_call_site_coverage`` is the load-bearing
guard until a fixture is vendored alongside the postmortem
evidence).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


_ANCHOR_DIR = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "runid_analysis"
    / "1099b152-8655-4f1e-ab43-1240a9400280"
    / "evidence"
)
_FIXTURE_CANDIDATES = (
    "replay_fixture_from_latest_export_1105451933925748.json",
    "replay_fixture_1105451933925748.json",
    "replay_fixture.json",
)


def _locate_fixture() -> Path | None:
    for name in _FIXTURE_CANDIDATES:
        path = _ANCHOR_DIR / name
        if path.exists():
            return path
    return None


@pytest.mark.skipif(
    _locate_fixture() is None,
    reason="airline anchor 13 replay fixture not vendored",
)
def test_airline_anchor_13_normalize_stage_capture_handles_lists() -> None:
    """Sanity replay: when the assembler-shaped fixture carries
    list-valued stage captures, ``_normalize_stage_capture`` returns
    a dict and downstream ``.get()`` is safe."""
    fixture_path = _locate_fixture()
    assert fixture_path is not None  # mypy/safety
    fixture = json.loads(fixture_path.read_text())
    from genie_space_optimizer.optimization.run_output_bundle import (
        _normalize_stage_capture,
    )

    # The fixture carries iteration entries with stage captures; the
    # contract is that any list-valued capture survives normalization
    # without raising. Walk the fixture defensively and prove it.
    for iteration in (fixture.get("iterations") or []):
        for stage_key, capture in (iteration.get("stages") or {}).items():
            normalized = _normalize_stage_capture(
                capture,
                stage_key=str(stage_key),
                iteration=int(iteration.get("iteration") or 0),
            )
            # Downstream is safe to call .get() now.
            assert hasattr(normalized, "get")


def test_airline_anchor_13_assemble_bundle_for_replay_emits_no_failure_markers(
    capsys,
) -> None:
    """D-4 binary criterion: a fixture-driven end-to-end assembly emits
    zero GSO_BUNDLE_ASSEMBLY_FAILED_V1 markers. Skips gracefully if
    fixture not vendored."""
    fixture_path = _locate_fixture()
    if fixture_path is None:
        pytest.skip("airline anchor 13 replay fixture not vendored")
    from genie_space_optimizer.optimization.run_output_bundle import (
        assemble_bundle_for_replay,
    )
    fixture = json.loads(fixture_path.read_text())
    result = assemble_bundle_for_replay(fixture)

    out = capsys.readouterr().out
    assert "GSO_BUNDLE_ASSEMBLY_FAILED_V1" not in out, (
        f"assembler emitted failure markers:\n{out}"
    )

    # RCO-1 — parent-bundle parity contract. Every canonical parent-level
    # key (minus operator_transcript / replay_fixture, which the seam
    # intentionally excludes) must be present. The synthetic-fixture
    # version of this assertion lives in
    # tests/unit/test_replay_assembler_parent_bundle_parity.py.
    from genie_space_optimizer.optimization.run_output_contract import (
        bundle_artifact_paths,
    )
    _EXCLUDED = {"operator_transcript", "replay_fixture"}
    canonical = {
        k for k in bundle_artifact_paths(iterations=[1]).keys()
        if k != "iterations"
    } - _EXCLUDED
    canonical.add("iteration_summaries")
    missing = canonical - set(result.keys())
    assert not missing, (
        f"Airline fixture replay missing parent-level keys: {sorted(missing)}"
    )

    # The seam must still produce a well-shaped bundle dict.
    assert isinstance(result["manifest"].get("iterations"), list)
    assert isinstance(result["failure_buckets"], dict)
    assert result["failure_buckets"].get("schema_version") == "v1"
