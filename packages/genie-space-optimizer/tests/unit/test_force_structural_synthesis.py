"""Forced structural synthesis for SQL-shape RCAs.

Today run_cluster_driven_synthesis_for_single_cluster returns dict
or None. The caller can't tell whether None means 'no archetype
matched', 'budget exhausted', or 'arbiter gate rejected'. This suite
pins the new ClusterSynthesisResult dataclass with an
``attempted_archetypes`` provenance list.
"""
from __future__ import annotations


def test_cluster_synthesis_result_dataclass_exists() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )

    r = ClusterSynthesisResult(
        proposal=None,
        attempted_archetypes=("ordered_list_by_metric",),
        skipped_reason="archetype_validation_failed",
    )
    assert r.proposal is None
    assert r.attempted_archetypes == ("ordered_list_by_metric",)
    assert r.skipped_reason == "archetype_validation_failed"


def test_synthesis_returns_result_object_not_dict_or_none() -> None:
    """Returns a ClusterSynthesisResult; ``proposal`` field carries
    the legacy ``dict`` payload (or None).
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
        run_cluster_driven_synthesis_for_single_cluster,
    )

    cluster = {
        "cluster_id": "H001",
        "question_ids": ["gs_026"],
        "root_cause": "plural_top_n_collapse",
    }
    metadata_snapshot = {
        "_space_id": "test_space",
        "_cluster_synthesis_count": 999,  # force budget skip
    }
    result = run_cluster_driven_synthesis_for_single_cluster(
        cluster, metadata_snapshot, benchmarks=[],
    )
    assert isinstance(result, ClusterSynthesisResult)
    assert result.proposal is None
    assert "budget" in (result.skipped_reason or "")


def test_force_structural_synthesis_flag_default_on() -> None:
    from genie_space_optimizer.common.config import (
        force_structural_synthesis_on_lever5_drop_enabled,
    )

    # Default ON when env var is unset.
    import os
    os.environ.pop("GSO_FORCE_STRUCTURAL_SYNTHESIS_ON_LEVER5_DROP", None)
    assert force_structural_synthesis_on_lever5_drop_enabled() is True


def test_force_structural_synthesis_flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_FORCE_STRUCTURAL_SYNTHESIS_ON_LEVER5_DROP", "0")
    from genie_space_optimizer.common.config import (
        force_structural_synthesis_on_lever5_drop_enabled,
    )
    assert force_structural_synthesis_on_lever5_drop_enabled() is False


def test_lever5_drop_invokes_synthesis_when_flag_on(monkeypatch) -> None:
    """Reproducer for run 2423b960 iter 2: lever-5 gate drops the
    instruction-only proposal for missing_filter, harness mandatorily
    invokes synthesis. The exact path through the harness is large;
    this test exercises the helper that decides whether to invoke
    synthesis given a gate-drop record + cluster root cause.
    """
    monkeypatch.setenv("GSO_FORCE_STRUCTURAL_SYNTHESIS_ON_LEVER5_DROP", "1")
    from genie_space_optimizer.optimization.harness import (
        _should_force_structural_synthesis,
    )

    assert _should_force_structural_synthesis(
        gate_drop_reason="lever5_structural_sql_shape_no_example_sql",
        cluster_root_cause="missing_filter",
    ) is True
    assert _should_force_structural_synthesis(
        gate_drop_reason="lever5_structural_sql_shape_no_example_sql",
        cluster_root_cause="plural_top_n_collapse",
    ) is True
    # Non-SQL-shape root cause does not force synthesis.
    assert _should_force_structural_synthesis(
        gate_drop_reason="lever5_structural_sql_shape_no_example_sql",
        cluster_root_cause="format_difference",
    ) is False
    # Different gate drop reason: not the lever-5 structural gate.
    assert _should_force_structural_synthesis(
        gate_drop_reason="blast_radius",
        cluster_root_cause="missing_filter",
    ) is False


def test_lever5_drop_does_not_invoke_synthesis_when_flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_FORCE_STRUCTURAL_SYNTHESIS_ON_LEVER5_DROP", "0")
    from genie_space_optimizer.optimization.harness import (
        _should_force_structural_synthesis,
    )
    assert _should_force_structural_synthesis(
        gate_drop_reason="lever5_structural_sql_shape_no_example_sql",
        cluster_root_cause="missing_filter",
    ) is False


def test_synthesized_proposal_buffered_for_next_iteration(monkeypatch) -> None:
    """A synthesized add_example_sql proposal must appear in the
    next iteration's strategist input as a forced AG candidate.
    """
    monkeypatch.setenv("GSO_FORCE_STRUCTURAL_SYNTHESIS_ON_LEVER5_DROP", "1")
    from genie_space_optimizer.optimization.harness import (
        _consume_structural_synthesis_buffer,
    )

    buffer = [
        {
            "proposal_id": "SYN_001",
            "patch_type": "add_example_sql",
            "target_qids": ["gs_021"],
            "_archetype_name": "ordered_list_by_metric",
        },
    ]
    consumed = _consume_structural_synthesis_buffer(buffer)
    assert len(consumed) == 1
    assert consumed[0]["patch_type"] == "add_example_sql"
    assert buffer == [], "buffer must be drained on consume"
