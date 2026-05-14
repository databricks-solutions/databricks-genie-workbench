"""Unit tests for Plan 3 (Three-Stage Pipeline). Sections:
  1. Flag helpers.
  1b. _THREE_STAGE_SKILL_NAMES registry.
  2. STAGE_1_DISCOVERY_PROMPT shape + render.
  3. _ThreeStageCaptureSink + atexit coverage gate.
  4. ActivationBundle dataclass + merge_skill_picks helper.
  5. build_activation_bundle.
  6. _call_llm_for_stage_1_discovery.
  7. Stage-2 dispatcher (_stage_2_for_skill + per-skill adapters).
  8. run_three_stage_pipeline_for_ag orchestrator.
  9. Shadow-mode emission + harness wiring.
"""
from __future__ import annotations

import importlib
import os


_PLAN3_ENV_KEYS = (
    "GSO_THREE_STAGE_V1",
    "GSO_THREE_STAGE_SHADOW_V1",
    "GSO_THREE_STAGE_CAPTURE_PATH",
    "GSO_THREE_STAGE_CAPTURE_REQUIRE_COVERAGE",
)


def _reload_config_with_env(env: dict[str, str]):
    """Reload common.config with patched env. Mirrors Plans 1 + 2 pattern.

    Env is set directly (no context manager) so subsequent calls to
    ``cfg.three_stage_*_enabled()`` after this function returns see the
    same env. Plan-3 env keys not in ``env`` are cleared so tests stay
    isolated.
    """
    from genie_space_optimizer.common import config as cfg

    for key in _PLAN3_ENV_KEYS:
        if key in env:
            os.environ[key] = env[key]
        else:
            os.environ.pop(key, None)
    return importlib.reload(cfg)


# ── Section 1: flag helpers ──────────────────────────────────────────


def test_three_stage_default_off():
    cfg = _reload_config_with_env({})
    assert cfg.three_stage_enabled() is False
    assert cfg.three_stage_shadow_enabled() is False
    assert cfg.three_stage_capture_path_set() is False
    assert cfg.three_stage_capture_require_coverage_enabled() is False


def test_three_stage_pipeline_flag_on():
    cfg = _reload_config_with_env({"GSO_THREE_STAGE_V1": "1"})
    assert cfg.three_stage_enabled() is True
    assert cfg.three_stage_shadow_enabled() is False


def test_three_stage_shadow_flag_on():
    cfg = _reload_config_with_env({"GSO_THREE_STAGE_SHADOW_V1": "1"})
    assert cfg.three_stage_enabled() is False
    assert cfg.three_stage_shadow_enabled() is True


def test_three_stage_capture_path_set_when_var_present():
    cfg = _reload_config_with_env({"GSO_THREE_STAGE_CAPTURE_PATH": "/tmp/x.ndjson"})
    assert cfg.three_stage_capture_path_set() is True


# ── Section 1b: skill name registry ───────────────────────────────────


def test_three_stage_skill_names_contains_expected():
    cfg = _reload_config_with_env({})
    expected = {
        "lever-1-table-column-description",
        "lever-2-mv-column-refinement",
        "lever-3-tvf-routing",
        "lever-4-join-discovery",
        "lever-5a-instructions",
        "lever-5b-example-sql",
        "lever-6-sql-expression",
    }
    assert cfg._THREE_STAGE_SKILL_NAMES == frozenset(expected)


def test_three_stage_skill_names_is_frozenset():
    cfg = _reload_config_with_env({})
    assert isinstance(cfg._THREE_STAGE_SKILL_NAMES, frozenset)


# ── Section 4: ActivationBundle ───────────────────────────────────────


def test_activation_bundle_required_fields():
    from genie_space_optimizer.optimization.activation_bundle import (
        ActivationBundle,
    )
    bundle = ActivationBundle(
        skill_id="lever-4-join-discovery",
        ag_id="AG1",
        target_objects=("catalog.schema.fact_orders", "catalog.schema.dim_customer"),
        cluster_afs=({"cluster_id": "C1", "failure_type": "missing_join"},),
        metadata_snapshot={"tables": [], "metric_views": [], "functions": []},
        identifier_allowlist="catalog.schema.fact_orders, catalog.schema.dim_customer",
        evidence_refs=("trace://q42",),
        expected_impact_qids=("Q42", "Q43"),
        raw_evidence=(),
        lever_directives_legacy=None,
        discovery_rationale="missing join between orders and customer",
        priority=1,
    )
    assert bundle.skill_id == "lever-4-join-discovery"
    assert bundle.ag_id == "AG1"
    assert bundle.priority == 1


def test_activation_bundle_is_frozen():
    import dataclasses
    from genie_space_optimizer.optimization.activation_bundle import (
        ActivationBundle,
    )
    bundle = ActivationBundle(
        skill_id="lever-4-join-discovery",
        ag_id="AG1",
        target_objects=(),
        cluster_afs=(),
        metadata_snapshot={},
        identifier_allowlist="",
        evidence_refs=(),
        expected_impact_qids=(),
        raw_evidence=(),
        lever_directives_legacy=None,
        discovery_rationale="",
        priority=1,
    )
    import pytest
    with pytest.raises(dataclasses.FrozenInstanceError):
        bundle.skill_id = "lever-1-table-column-description"


def test_activation_bundle_equality_by_skill_ag_targets():
    from genie_space_optimizer.optimization.activation_bundle import (
        ActivationBundle,
    )
    a = ActivationBundle(
        skill_id="lever-1-table-column-description",
        ag_id="AG1",
        target_objects=("catalog.schema.t1",),
        cluster_afs=(),
        metadata_snapshot={},
        identifier_allowlist="",
        evidence_refs=("trace://q1",),
        expected_impact_qids=("Q1",),
        raw_evidence=(),
        lever_directives_legacy=None,
        discovery_rationale="r1",
        priority=1,
    )
    b = ActivationBundle(
        skill_id="lever-1-table-column-description",
        ag_id="AG1",
        target_objects=("catalog.schema.t1",),
        cluster_afs=(),
        metadata_snapshot={},
        identifier_allowlist="",
        evidence_refs=("trace://q2",),
        expected_impact_qids=("Q9",),
        raw_evidence=(),
        lever_directives_legacy=None,
        discovery_rationale="r2",
        priority=2,
    )
    assert a == b
    assert hash(a) == hash(b)


def test_activation_bundle_target_objects_must_be_tuple():
    """Frozen dataclass + tuple gives free hashability. List would
    break hash() and the equality semantics; reject at construction."""
    import pytest
    from genie_space_optimizer.optimization.activation_bundle import (
        ActivationBundle,
    )
    with pytest.raises(TypeError, match="target_objects must be tuple"):
        ActivationBundle(
            skill_id="lever-1-table-column-description",
            ag_id="AG1",
            target_objects=["catalog.schema.t1"],  # noqa: invalid
            cluster_afs=(),
            metadata_snapshot={},
            identifier_allowlist="",
            evidence_refs=(),
            expected_impact_qids=(),
            raw_evidence=(),
            lever_directives_legacy=None,
            discovery_rationale="",
            priority=1,
        )


def test_activation_bundle_priority_must_be_in_1_2_3():
    import pytest
    from genie_space_optimizer.optimization.activation_bundle import (
        ActivationBundle,
    )
    with pytest.raises(ValueError, match="priority"):
        ActivationBundle(
            skill_id="lever-1-table-column-description",
            ag_id="AG1",
            target_objects=(),
            cluster_afs=(),
            metadata_snapshot={},
            identifier_allowlist="",
            evidence_refs=(),
            expected_impact_qids=(),
            raw_evidence=(),
            lever_directives_legacy=None,
            discovery_rationale="",
            priority=99,
        )


def test_merge_skill_picks_collapses_same_skill_id():
    from genie_space_optimizer.optimization.activation_bundle import (
        merge_skill_picks,
    )
    picks = [
        {"skill_id": "lever-1-table-column-description",
         "target_objects": ["catalog.schema.t1"],
         "evidence_refs": ["trace://q1"], "expected_impact_qids": ["Q1"],
         "why": "missing description on t1", "priority": 1},
        {"skill_id": "lever-1-table-column-description",
         "target_objects": ["catalog.schema.t2"],
         "evidence_refs": ["trace://q2"], "expected_impact_qids": ["Q2"],
         "why": "missing description on t2", "priority": 2},
        {"skill_id": "lever-4-join-discovery",
         "target_objects": ["catalog.schema.t1", "catalog.schema.t2"],
         "evidence_refs": ["trace://q3"], "expected_impact_qids": ["Q3"],
         "why": "missing join", "priority": 1},
    ]
    merged = merge_skill_picks(picks)
    assert len(merged) == 2
    by_id = {m["skill_id"]: m for m in merged}
    assert sorted(by_id["lever-1-table-column-description"]["target_objects"]) == [
        "catalog.schema.t1", "catalog.schema.t2",
    ]
    assert sorted(by_id["lever-1-table-column-description"]["expected_impact_qids"]) == ["Q1", "Q2"]
    # Priority is the MIN of merged picks (highest urgency wins):
    assert by_id["lever-1-table-column-description"]["priority"] == 1
    # Why is concatenated:
    assert "missing description on t1" in by_id["lever-1-table-column-description"]["why"]
    assert "missing description on t2" in by_id["lever-1-table-column-description"]["why"]


def test_merge_skill_picks_handles_empty_input():
    from genie_space_optimizer.optimization.activation_bundle import (
        merge_skill_picks,
    )
    assert merge_skill_picks([]) == []


def test_merge_skill_picks_preserves_distinct_skill_ids():
    from genie_space_optimizer.optimization.activation_bundle import (
        merge_skill_picks,
    )
    picks = [
        {"skill_id": "a", "target_objects": ["x"], "evidence_refs": [],
         "expected_impact_qids": [], "why": "ax", "priority": 1},
        {"skill_id": "b", "target_objects": ["y"], "evidence_refs": [],
         "expected_impact_qids": [], "why": "by", "priority": 1},
        {"skill_id": "c", "target_objects": ["z"], "evidence_refs": [],
         "expected_impact_qids": [], "why": "cz", "priority": 1},
    ]
    out = merge_skill_picks(picks)
    assert sorted(o["skill_id"] for o in out) == ["a", "b", "c"]


# ── Section 5: build_activation_bundle ────────────────────────────────


def _sample_metadata_snapshot() -> dict:
    return {
        "config": {"description": "Hotel bookings analytics"},
        "data_sources": {
            "tables": [{
                "name": "catalog.schema.fact_bookings",
                "column_configs": [
                    {"name": "booking_id"},
                    {"name": "booking_date"},
                ],
            }],
            "metric_views": [],
            "functions": [],
        },
        "tables": [{
            "name": "catalog.schema.fact_bookings",
            "column_configs": [
                {"name": "booking_id"},
                {"name": "booking_date"},
            ],
        }],
        "metric_views": [],
        "functions": [],
        "instructions": {"text_instructions": []},
    }


def _sample_cluster() -> dict:
    return {
        "cluster_id": "C1",
        "root_cause": "missing_join_spec",
        "asi_failure_type": "missing_join",
        "asi_blame_set": ["catalog.schema.fact_bookings.hotel_key"],
        "question_ids": ["Q1", "Q2"],
        "question_traces": [
            {"question_id": "Q1", "trace_id": "trace://q1"},
            {"question_id": "Q2", "trace_id": "trace://q2"},
        ],
    }


def test_build_bundle_returns_activation_bundle_with_correct_skill_id():
    from genie_space_optimizer.optimization.activation_bundle import (
        ActivationBundle, build_activation_bundle,
    )
    pick = {
        "skill_id": "lever-4-join-discovery",
        "target_objects": ["catalog.schema.fact_bookings"],
        "evidence_refs": ["trace://q1"],
        "expected_impact_qids": ["Q1"],
        "why": "missing join",
        "priority": 1,
    }
    bundle = build_activation_bundle(
        pick=pick,
        ag_id="AG1",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
    )
    assert isinstance(bundle, ActivationBundle)
    assert bundle.skill_id == "lever-4-join-discovery"
    assert bundle.ag_id == "AG1"
    assert bundle.target_objects == ("catalog.schema.fact_bookings",)
    assert bundle.priority == 1
    assert bundle.discovery_rationale == "missing join"


def test_build_bundle_serializes_clusters_through_format_afs():
    """The builder must call optimization.afs.format_afs on each cluster."""
    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle,
    )
    pick = {
        "skill_id": "lever-4-join-discovery",
        "target_objects": [],
        "evidence_refs": [],
        "expected_impact_qids": [],
        "why": "",
        "priority": 1,
    }
    bundle = build_activation_bundle(
        pick=pick,
        ag_id="AG1",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
    )
    assert len(bundle.cluster_afs) == 1
    afs = bundle.cluster_afs[0]
    assert afs.get("cluster_id") == "C1"


def test_build_bundle_renders_identifier_allowlist():
    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle,
    )
    pick = {
        "skill_id": "lever-1-table-column-description",
        "target_objects": ["catalog.schema.fact_bookings"],
        "evidence_refs": [],
        "expected_impact_qids": [],
        "why": "",
        "priority": 1,
    }
    bundle = build_activation_bundle(
        pick=pick,
        ag_id="AG1",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
    )
    assert bundle.identifier_allowlist  # non-empty
    assert "fact_bookings" in bundle.identifier_allowlist


def test_build_bundle_raw_evidence_always_empty_in_plan_3():
    """Plan 3 explicitly does NOT populate raw_evidence — that's Plan 4."""
    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle,
    )
    pick = {
        "skill_id": "lever-4-join-discovery",
        "target_objects": [],
        "evidence_refs": [],
        "expected_impact_qids": [],
        "why": "",
        "priority": 1,
    }
    bundle = build_activation_bundle(
        pick=pick,
        ag_id="AG1",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
    )
    assert bundle.raw_evidence == ()


def test_build_bundle_lever_directives_legacy_default_none():
    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle,
    )
    pick = {
        "skill_id": "lever-4-join-discovery",
        "target_objects": [],
        "evidence_refs": [],
        "expected_impact_qids": [],
        "why": "",
        "priority": 1,
    }
    bundle = build_activation_bundle(
        pick=pick,
        ag_id="AG1",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
    )
    assert bundle.lever_directives_legacy is None


def test_build_bundle_target_objects_deduped_and_sorted():
    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle,
    )
    pick = {
        "skill_id": "lever-1-table-column-description",
        "target_objects": ["b", "a", "a", "c", "b"],
        "evidence_refs": [],
        "expected_impact_qids": [],
        "why": "",
        "priority": 1,
    }
    bundle = build_activation_bundle(
        pick=pick,
        ag_id="AG1",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
    )
    assert bundle.target_objects == ("a", "b", "c")
