"""Pin the Plan P-A contract change: per-iteration ``stages`` declared
path is now a leaf (``stages/index.json``) so MLflow listing-based
completeness checks can satisfy it.

Before P-A the entry was a directory string (``stages``), which the
listing-based ``assembler_completeness_check`` could never satisfy
because MLflow returns leaf files only — the path was *always* in
``unmigrated_per_iteration_missing``.
"""
from __future__ import annotations


def test_bundle_artifact_paths_declares_stages_index_leaf() -> None:
    from genie_space_optimizer.optimization.run_output_contract import (
        bundle_artifact_paths,
    )
    paths = bundle_artifact_paths(iterations=[1, 2])
    iter_1_paths = paths["iterations"][1]
    assert iter_1_paths["stages"] == (
        "gso_postmortem_bundle/iterations/iter_01/stages/index.json"
    )
    iter_2_paths = paths["iterations"][2]
    assert iter_2_paths["stages"] == (
        "gso_postmortem_bundle/iterations/iter_02/stages/index.json"
    )


def test_build_artifact_index_declares_stages_index_leaf() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_artifact_index,
    )
    index = build_artifact_index(iterations=[1])
    iter_1 = index["iterations"]["1"]
    # Old behaviour: ``stages`` was a dict of stage-key -> per-stage paths.
    # New behaviour (P-A): ``stages_index`` is a string leaf so
    # ``assembler_completeness_check`` can satisfy it via MLflow listing.
    # The per-stage map remains under ``stage_artifacts`` so the postmortem
    # skill still has structured paths to walk.
    assert iter_1["stages_index"] == (
        "gso_postmortem_bundle/iterations/iter_01/stages/index.json"
    )
    assert "stage_artifacts" in iter_1
    assert "01_evaluation_state" in iter_1["stage_artifacts"]


def test_assembler_completeness_check_satisfied_by_stages_index_leaf() -> None:
    """Regression: with the leaf-path contract, listing
    ``stages/index.json`` is enough to mark the per-iter ``stages`` slot
    materialized."""
    from genie_space_optimizer.optimization.run_output_contract import (
        assembler_completeness_check,
        bundle_artifact_paths,
    )
    paths = bundle_artifact_paths(iterations=[1])
    declared: list[str] = []
    for k, v in paths.items():
        if k == "iterations":
            for iter_paths in v.values():
                for p in iter_paths.values():
                    if isinstance(p, str):
                        declared.append(p)
        elif isinstance(v, str):
            declared.append(v)
    # Materialize every declared path including the new stages index.
    materialized = list(declared)
    report = assembler_completeness_check(
        declared_paths=declared, materialized_paths=materialized,
    )
    assert report["complete"] is True
    assert report["unmigrated_per_iteration_missing"] == []
