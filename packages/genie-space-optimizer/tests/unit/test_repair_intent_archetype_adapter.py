"""Plan 1 Task 3 — Archetype → RepairIntent adapter contract.

The adapter is the deterministic producer of RepairIntents in Plan 1.
Plan 2 replaces the L5b call site of this adapter with an LLM call;
the adapter itself stays as the fallback / non-L5b synthesis path
producer.

The mapping from Archetype.name to RepairShape is hard-coded here
(not derived) so the catalog stays the source of truth for archetypes
and the adapter stays the source of truth for repair_shape semantics.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.archetypes import ARCHETYPES, Archetype
from genie_space_optimizer.optimization.failure_cluster import FailureCluster
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
    intent_from_archetype,
)


def _sample_cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H001",
        target_qids=("gs_009",),
        root_cause="plural_top_n_collapse",
        asi_failure_type="plural_top_n_collapse",
        failure_keys=("plural_top_n_collapse",),
        blame_set_raw=("flights.carrier",),
        blame_set_normalized=("flights.carrier",),
        rca_card_id="rca_v1",
        rca_card_summary="needs top-n shape",
        is_grounded=True,
    )


def _find(name: str) -> Archetype:
    for a in ARCHETYPES:
        if a.name == name:
            return a
    raise AssertionError(f"archetype {name!r} not in catalog")


def test_adapter_returns_repair_intent() -> None:
    arch = _find("top_n_by_metric")
    cluster = _sample_cluster()
    intent = intent_from_archetype(
        archetype=arch,
        cluster=cluster,
        ag_id="AG_H001_L5",
        seq=1,
    )
    assert isinstance(intent, RepairIntent)


def test_intent_id_is_stable_and_typed_for_postmortem() -> None:
    arch = _find("top_n_by_metric")
    cluster = _sample_cluster()
    a = intent_from_archetype(archetype=arch, cluster=cluster, ag_id="AG_X", seq=1)
    b = intent_from_archetype(archetype=arch, cluster=cluster, ag_id="AG_X", seq=1)
    assert a.intent_id == b.intent_id, "adapter must be deterministic"
    assert a.intent_id == "intent_H001_AG_X_top_n_by_metric_001"


def test_intent_carries_archetype_name_and_description() -> None:
    arch = _find("top_n_by_metric")
    cluster = _sample_cluster()
    intent = intent_from_archetype(
        archetype=arch, cluster=cluster, ag_id="AG_X", seq=1,
    )
    assert intent.intent_name == "top_n_by_metric"
    assert "Top-N" in intent.intent_description


def test_intent_carries_cluster_provenance() -> None:
    arch = _find("top_n_by_metric")
    cluster = _sample_cluster()
    intent = intent_from_archetype(
        archetype=arch, cluster=cluster, ag_id="AG_H001_L5", seq=1,
    )
    assert intent.cluster_id == "H001"
    assert intent.target_qids == ("gs_009",)
    assert intent.blame_set == ("flights.carrier",)
    assert intent.rca_card_id == "rca_v1"
    assert intent.ag_id == "AG_H001_L5"


def test_intent_carries_source_marker() -> None:
    arch = _find("top_n_by_metric")
    cluster = _sample_cluster()
    intent = intent_from_archetype(
        archetype=arch, cluster=cluster, ag_id="AG_X", seq=1,
    )
    assert intent.source == "deterministic_archetype_adapter"


def test_intent_patch_type_matches_archetype() -> None:
    arch_example = _find("top_n_by_metric")
    arch_filter = _find("filter_compose")
    cluster = _sample_cluster()
    a = intent_from_archetype(
        archetype=arch_example, cluster=cluster, ag_id="AG_X", seq=1,
    )
    b = intent_from_archetype(
        archetype=arch_filter, cluster=cluster, ag_id="AG_X", seq=1,
    )
    assert a.patch_type is PatchType.ADD_EXAMPLE_SQL
    assert b.patch_type is PatchType.ADD_SQL_SNIPPET_FILTER


@pytest.mark.parametrize(
    "archetype_name,expected_shape",
    [
        ("simple_enumerate", RepairShape.OTHER),
        ("ordered_list_by_metric", RepairShape.ORDERED_LIST_BY_METRIC),
        ("top_n_by_metric", RepairShape.TOP_N_BY_METRIC),
        ("group_by_all_projected_keys", RepairShape.OTHER),
        ("period_over_period", RepairShape.PERIOD_OVER_PERIOD),
        ("correct_join_spec", RepairShape.JOIN_DISCOVERY),
        ("cohort_retention", RepairShape.OTHER),
        ("funnel_conversion", RepairShape.OTHER),
        ("ratio_by_dimension", RepairShape.OTHER),
        ("running_total", RepairShape.OTHER),
        ("rank_within_group", RepairShape.RANK_WITHIN_GROUP),
        ("pct_change", RepairShape.PERIOD_OVER_PERIOD),
        ("filter_compose", RepairShape.FILTER_COMPOSE),
        ("segment_compare", RepairShape.OTHER),
        ("disambiguate_column", RepairShape.COLUMN_DESCRIPTION),
        ("time_window_aggregate", RepairShape.PERIOD_OVER_PERIOD),
        ("self_join_hierarchy", RepairShape.JOIN_DISCOVERY),
        ("event_sequence", RepairShape.OTHER),
        ("distinct_count_by_dim", RepairShape.OTHER),
        ("pivot_wide", RepairShape.OTHER),
    ],
)
def test_every_catalog_archetype_maps_to_a_repair_shape(
    archetype_name: str, expected_shape: RepairShape,
) -> None:
    """Every Archetype in the catalog must have an explicit RepairShape
    mapping (even if that mapping is OTHER). New archetypes added to
    the catalog must be added here in the same commit."""
    arch = _find(archetype_name)
    cluster = _sample_cluster()
    intent = intent_from_archetype(
        archetype=arch, cluster=cluster, ag_id="AG_X", seq=1,
    )
    assert intent.repair_shape is expected_shape


def test_adapter_covers_every_catalog_archetype() -> None:
    """Catalog drift detector: if a new archetype is added without an
    adapter mapping, the adapter raises KeyError and this test fails."""
    cluster = _sample_cluster()
    for arch in ARCHETYPES:
        intent_from_archetype(
            archetype=arch, cluster=cluster, ag_id="AG_X", seq=1,
        )


def test_adapter_rationale_includes_cluster_root_cause() -> None:
    arch = _find("top_n_by_metric")
    cluster = _sample_cluster()
    intent = intent_from_archetype(
        archetype=arch, cluster=cluster, ag_id="AG_X", seq=1,
    )
    assert "plural_top_n_collapse" in intent.rationale
