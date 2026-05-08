"""Cycle 11 — Phase H declared-vs-materialized path validator.

Pure helper: takes (declared_paths, materialized_paths) and returns
a list of ``manifest_path_missing`` dicts for the difference. The
harness wires it after _build_manifest using the live MLflow listing.
"""

from __future__ import annotations


def test_validator_reports_missing_paths() -> None:
    from genie_space_optimizer.optimization.run_output_contract import (
        validate_phase_h_manifest_paths,
    )

    declared = [
        "gso_postmortem_bundle/decision_trace_all.json",
        "gso_postmortem_bundle/iterations/iter_01/summary.json",
        "gso_postmortem_bundle/manifest.json",
    ]
    materialized = [
        "gso_postmortem_bundle/manifest.json",
    ]

    missing = validate_phase_h_manifest_paths(
        declared_paths=declared, materialized_paths=materialized,
    )
    paths = sorted(m["artifact_path"] for m in missing)
    assert paths == [
        "gso_postmortem_bundle/decision_trace_all.json",
        "gso_postmortem_bundle/iterations/iter_01/summary.json",
    ]
    assert all(m["kind"] == "manifest_path_missing" for m in missing)


def test_validator_returns_empty_when_all_present() -> None:
    from genie_space_optimizer.optimization.run_output_contract import (
        validate_phase_h_manifest_paths,
    )

    declared = ["a", "b"]
    materialized = ["a", "b", "c"]
    assert validate_phase_h_manifest_paths(
        declared_paths=declared, materialized_paths=materialized,
    ) == []


def test_phase_h_manifest_strict_validation_flag_default_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PHASE_H_MANIFEST_STRICT_VALIDATION", raising=False)
    from genie_space_optimizer.common.config import (
        phase_h_manifest_strict_validation_enabled,
    )
    assert phase_h_manifest_strict_validation_enabled() is True


def test_validate_phase_h_manifest_paths_excludes_self_write_paths() -> None:
    """Paths the caller declares as ``self_write_paths`` are treated as
    materialized — even when MLflow listing returned them as absent — so
    the validator does not false-flag the assembler's imminently-written
    artifacts as missing."""
    from genie_space_optimizer.optimization.run_output_contract import (
        validate_phase_h_manifest_paths,
    )

    declared = [
        "gso_postmortem_bundle/manifest.json",
        "gso_postmortem_bundle/run_summary.json",
        "gso_postmortem_bundle/operator_transcript.md",
        "gso_postmortem_bundle/artifact_index.json",
        "gso_postmortem_bundle/replay_fixture.json",
    ]
    materialized: list[str] = []  # listing happens BEFORE assembler writes
    self_writes = {
        "gso_postmortem_bundle/manifest.json",
        "gso_postmortem_bundle/run_summary.json",
        "gso_postmortem_bundle/operator_transcript.md",
        "gso_postmortem_bundle/artifact_index.json",
    }

    missing = validate_phase_h_manifest_paths(
        declared_paths=declared,
        materialized_paths=materialized,
        self_write_paths=self_writes,
    )

    # Only replay_fixture.json (declared, neither materialized nor self-write)
    # is reported missing — the four self-write paths are excluded.
    assert len(missing) == 1
    assert missing[0]["artifact_path"] == "gso_postmortem_bundle/replay_fixture.json"
    assert missing[0]["kind"] == "manifest_path_missing"


def test_validate_phase_h_manifest_paths_self_write_param_is_optional() -> None:
    """Calling without ``self_write_paths`` keeps the legacy behaviour
    (every declared-but-absent path is missing)."""
    from genie_space_optimizer.optimization.run_output_contract import (
        validate_phase_h_manifest_paths,
    )

    missing = validate_phase_h_manifest_paths(
        declared_paths=["a", "b"],
        materialized_paths=["a"],
    )

    assert len(missing) == 1
    assert missing[0]["artifact_path"] == "b"


# Cycle 12-T3 — assembler_completeness_check
def test_assembler_completeness_check_categorizes_missing_paths() -> None:
    from genie_space_optimizer.optimization.run_output_contract import (
        assembler_completeness_check,
    )

    declared = [
        "gso_postmortem_bundle/manifest.json",
        "gso_postmortem_bundle/replay_fixture.json",
        "gso_postmortem_bundle/iterations/iter_01/decision_trace.json",
        "gso_postmortem_bundle/iterations/iter_01/operator_transcript.md",
    ]
    materialized = [
        "gso_postmortem_bundle/manifest.json",
        # replay_fixture.json missing → parent_level
        # iterations/iter_01/decision_trace.json missing → unmigrated_per_iteration
        # iterations/iter_01/operator_transcript.md missing → unmigrated_per_iteration
    ]

    report = assembler_completeness_check(
        declared_paths=declared,
        materialized_paths=materialized,
    )

    assert report["total_declared"] == 4
    assert report["total_materialized"] == 1
    assert report["missing_count"] == 3
    assert report["parent_level_missing"] == [
        "gso_postmortem_bundle/replay_fixture.json",
    ]
    assert sorted(report["unmigrated_per_iteration_missing"]) == [
        "gso_postmortem_bundle/iterations/iter_01/decision_trace.json",
        "gso_postmortem_bundle/iterations/iter_01/operator_transcript.md",
    ]
    assert report["complete"] is False


def test_assembler_completeness_check_returns_complete_when_all_materialized() -> None:
    from genie_space_optimizer.optimization.run_output_contract import (
        assembler_completeness_check,
    )

    paths = ["gso_postmortem_bundle/manifest.json"]

    report = assembler_completeness_check(
        declared_paths=paths,
        materialized_paths=paths,
    )

    assert report["complete"] is True
    assert report["missing_count"] == 0
    assert report["parent_level_missing"] == []
    assert report["unmigrated_per_iteration_missing"] == []


def test_assembler_completeness_check_treats_paths_after_iterations_as_per_iteration() -> None:
    """Categorization uses the path's structure, not arbitrary heuristics."""
    from genie_space_optimizer.optimization.run_output_contract import (
        assembler_completeness_check,
    )

    declared = [
        "gso_postmortem_bundle/iterations/iter_05/stages/03_strategy/output.json",
    ]

    report = assembler_completeness_check(
        declared_paths=declared,
        materialized_paths=[],
    )

    assert report["unmigrated_per_iteration_missing"] == [
        "gso_postmortem_bundle/iterations/iter_05/stages/03_strategy/output.json",
    ]
    assert report["parent_level_missing"] == []
