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


# ── Section 2: STAGE_1_DISCOVERY_PROMPT ───────────────────────────────


def test_stage_1_discovery_prompt_exists_and_has_required_slots():
    cfg = _reload_config_with_env({})
    p = cfg.STAGE_1_DISCOVERY_PROMPT
    for slot in (
        "{{ space_description }}",
        "{{ ag_id }}",
        "{{ root_cause_summary }}",
        "{{ cluster_briefs }}",
        "{{ skill_catalogue }}",
        "{{ identifier_allowlist }}",
    ):
        assert slot in p, f"missing slot: {slot}"


def test_stage_1_discovery_prompt_lists_all_pickable_skills():
    """After Task 4, the unrendered template no longer lists skill_ids
    in a static block — they come from the runtime catalogue renderer.
    This test renders the template with the real catalogue and asserts
    every pickable skill_id surfaces post-render. Catches both
    catalogue gaps and template-slot regressions."""
    cfg = _reload_config_with_env({})
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_failure_type_routing_table,
        _render_rich_skill_catalogue,
    )
    rendered = cfg.format_mlflow_template(
        cfg.STAGE_1_DISCOVERY_PROMPT,
        space_description="(test)",
        ag_id="AG_T",
        root_cause_summary="(test)",
        cluster_briefs="(test)",
        skill_catalogue=_render_rich_skill_catalogue(),
        failure_type_routing_table=_render_failure_type_routing_table(),
        identifier_allowlist="(test)",
    )
    for skill_id in cfg._THREE_STAGE_SKILL_NAMES:
        assert skill_id in rendered, (
            f"skill {skill_id} missing from RENDERED discovery prompt"
        )


def test_stage_1_discovery_prompt_includes_rca_contract_header():
    """Discovery is causal — its choice gates downstream patches.
    Plan 1's narrowing flag does NOT remove the contract header from
    causal prompts, so the header substring is always present."""
    cfg = _reload_config_with_env({})
    assert "RCA" in cfg.STAGE_1_DISCOVERY_PROMPT or "Root Cause" in cfg.STAGE_1_DISCOVERY_PROMPT


def test_stage_1_discovery_prompt_output_schema_is_applicable_skills():
    """The output_schema block must define applicable_skills with
    skill_id, target_objects, expected_impact_qids, evidence_refs,
    why, priority."""
    cfg = _reload_config_with_env({})
    p = cfg.STAGE_1_DISCOVERY_PROMPT
    for key in ("applicable_skills", "target_objects",
                "expected_impact_qids", "evidence_refs",
                "priority", "why"):
        assert key in p, f"output_schema missing: {key}"


def test_stage_1_discovery_prompt_renders_with_realistic_kwargs():
    cfg = _reload_config_with_env({})
    rendered = cfg.format_mlflow_template(
        cfg.STAGE_1_DISCOVERY_PROMPT,
        space_description="Hotel bookings",
        ag_id="AG1",
        root_cause_summary="missing join between fact_bookings and dim_hotel",
        cluster_briefs="C1: missing_join — hotel_key not joined to dim_hotel",
        skill_catalogue="lever-4-join-discovery: ...",
        failure_type_routing_table="| failure_type | preferred skill_id(s) |",
        identifier_allowlist="catalog.schema.fact_bookings.hotel_key",
    )
    assert "Hotel bookings" in rendered
    assert "{{" not in rendered, "unrendered template variable"


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
            "tables": [
                {
                    "name": "catalog.schema.fact_bookings",
                    "column_configs": [
                        {"name": "booking_id"},
                        {"name": "booking_date"},
                    ],
                },
                {
                    "name": "catalog.schema.dim_hotel",
                    "column_configs": [
                        {"name": "hotel_id"},
                        {"name": "hotel_name"},
                    ],
                },
            ],
            "metric_views": [],
            "functions": [],
        },
        "tables": [
            {
                "name": "catalog.schema.fact_bookings",
                "column_configs": [
                    {"name": "booking_id"},
                    {"name": "booking_date"},
                ],
            },
            {
                "name": "catalog.schema.dim_hotel",
                "column_configs": [
                    {"name": "hotel_id"},
                    {"name": "hotel_name"},
                ],
            },
        ],
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


# ── Section 6: _call_llm_for_stage_1_discovery ────────────────────────


def test_call_llm_for_stage_1_discovery_returns_applicable_skills_shape(monkeypatch):
    from genie_space_optimizer.optimization import optimizer

    def _fake_llm_openai(*args, **kwargs):
        # Use lever-1 (min_count=0) with a target that matches the
        # snapshot's fact_bookings so the post-coercion allowlist
        # contract (Task 5, 2026-05-17 target-shape-constraints plan)
        # is satisfied.
        return (
            '{"applicable_skills": ['
            '{"skill_id": "lever-1-table-column-description",'
            ' "target_objects": ["catalog.schema.fact_bookings"],'
            ' "expected_impact_qids": ["Q1"],'
            ' "evidence_refs": ["trace://q1"],'
            ' "why": "missing column description", "priority": 1}'
            '], "discovery_rationale": "metadata gap"}',
            None,
        )
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    result = optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="missing join",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    assert "applicable_skills" in result
    assert isinstance(result["applicable_skills"], list)
    assert len(result["applicable_skills"]) == 1
    assert result["applicable_skills"][0]["skill_id"] == "lever-1-table-column-description"
    assert "discovery_rationale" in result


def test_call_llm_for_stage_1_discovery_filters_unknown_skill_ids(monkeypatch):
    """Out-of-set skill_ids must be dropped (logged + skipped). Empty
    applicable_skills after filtering is valid."""
    from genie_space_optimizer.optimization import optimizer

    def _fake_llm_openai(*args, **kwargs):
        return (
            '{"applicable_skills": ['
            '{"skill_id": "made-up-skill",'
            ' "target_objects": ["x"], "expected_impact_qids": [],'
            ' "evidence_refs": [], "why": "?", "priority": 1}'
            '], "discovery_rationale": "trying"}',
            None,
        )
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    result = optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="x",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    assert result["applicable_skills"] == []


def test_call_llm_for_stage_1_discovery_records_capture_when_flag_on(monkeypatch):
    cfg = _reload_config_with_env({"GSO_THREE_STAGE_V1": "1"})
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer

    def _fake_llm_openai(*args, **kwargs):
        return ('{"applicable_skills": [], "discovery_rationale": ""}', None)
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="x",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    snap = cfg.dump_three_stage_capture_summary()
    assert snap["discovery_calls"] == 1


def test_call_llm_for_stage_1_discovery_returns_empty_on_json_parse_failure(monkeypatch):
    """Discovery is best-effort. JSON parse failure → empty
    applicable_skills (caller falls back to legacy)."""
    from genie_space_optimizer.optimization import optimizer

    def _fake_llm_openai(*args, **kwargs):
        return ("not valid json", None)
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    result = optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="x",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    assert result == {"applicable_skills": [], "discovery_rationale": "JSON parse failed"}


def test_call_llm_for_stage_1_discovery_returns_empty_on_llm_failure(monkeypatch):
    """LLM call exception → empty (caller falls back to legacy)."""
    from genie_space_optimizer.optimization import optimizer

    def _fake_llm_openai(*args, **kwargs):
        raise RuntimeError("LLM endpoint down")
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    result = optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="x",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    assert result["applicable_skills"] == []
    assert "LLM call failed" in result["discovery_rationale"]


# ── Section 3: _ThreeStageCaptureSink ─────────────────────────────────


def test_three_stage_capture_sink_initial_state():
    cfg = _reload_config_with_env({})
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    snap = cfg.dump_three_stage_capture_summary()
    assert snap["discovery_calls"] == 0
    assert all(c == 0 for c in snap["skill_dispatches"].values())
    assert snap["shadow_comparisons"] == 0
    assert snap["all_required_sites_exercised"] is False


def test_three_stage_record_discovery_call_increments():
    cfg = _reload_config_with_env({})
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_three_stage_discovery_call("AG1")
    cfg._record_three_stage_discovery_call("AG2")
    snap = cfg.dump_three_stage_capture_summary()
    assert snap["discovery_calls"] == 2


def test_three_stage_record_skill_dispatch_increments_per_skill():
    cfg = _reload_config_with_env({})
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_three_stage_skill_dispatch("lever-4-join-discovery")
    cfg._record_three_stage_skill_dispatch("lever-4-join-discovery")
    cfg._record_three_stage_skill_dispatch("lever-1-table-column-description")
    snap = cfg.dump_three_stage_capture_summary()
    assert snap["skill_dispatches"]["lever-4-join-discovery"] == 2
    assert snap["skill_dispatches"]["lever-1-table-column-description"] == 1
    assert snap["skill_dispatches"]["lever-6-sql-expression"] == 0


def test_three_stage_record_shadow_comparison_writes_ndjson():
    import json
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "three_stage.ndjson"
        cfg = _reload_config_with_env({
            "GSO_THREE_STAGE_SHADOW_V1": "1",
            "GSO_THREE_STAGE_CAPTURE_PATH": str(path),
        })
        cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
        cfg._record_three_stage_shadow_comparison({
            "ag_id": "AG1",
            "stage_1_skill_ids": ["lever-4-join-discovery"],
            "legacy_lever_directives_keys": ["1", "4"],
            "structural_overlap": 0.5,
        })
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["ag_id"] == "AG1"
        assert "captured_at" in record
        assert "process_pid" in record


def test_three_stage_coverage_gate_passes_on_full_coverage():
    cfg = _reload_config_with_env({
        "GSO_THREE_STAGE_SHADOW_V1": "1",
        "GSO_THREE_STAGE_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_three_stage_discovery_call("AG1")
    cfg._record_three_stage_skill_dispatch("lever-4-join-discovery")
    cfg._record_three_stage_shadow_comparison({"ag_id": "AG1"})
    cfg._THREE_STAGE_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


def test_three_stage_coverage_gate_raises_when_no_discovery_call():
    import pytest
    cfg = _reload_config_with_env({
        "GSO_THREE_STAGE_SHADOW_V1": "1",
        "GSO_THREE_STAGE_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_three_stage_skill_dispatch("lever-4-join-discovery")
    cfg._record_three_stage_shadow_comparison({"ag_id": "AG1"})
    with pytest.raises(RuntimeError, match="zero Stage-1 discovery"):
        cfg._THREE_STAGE_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


def test_three_stage_coverage_gate_raises_when_no_skill_dispatched():
    import pytest
    cfg = _reload_config_with_env({
        "GSO_THREE_STAGE_SHADOW_V1": "1",
        "GSO_THREE_STAGE_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._record_three_stage_discovery_call("AG1")
    cfg._record_three_stage_shadow_comparison({"ag_id": "AG1"})
    with pytest.raises(RuntimeError, match="zero Stage-2 dispatch"):
        cfg._THREE_STAGE_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


# ── Section 7: _stage_2_for_skill (L4 first) ──────────────────────────


def _sample_bundle(skill_id: str = "lever-4-join-discovery"):
    from genie_space_optimizer.optimization.activation_bundle import (
        ActivationBundle,
    )
    from genie_space_optimizer.optimization.afs import format_afs
    return ActivationBundle(
        skill_id=skill_id,
        ag_id="AG1",
        target_objects=("catalog.schema.fact_bookings",
                        "catalog.schema.dim_hotel"),
        cluster_afs=(format_afs(_sample_cluster()),),
        metadata_snapshot=_sample_metadata_snapshot(),
        identifier_allowlist="catalog.schema.fact_bookings, catalog.schema.dim_hotel",
        evidence_refs=("trace://q1",),
        expected_impact_qids=("Q1",),
        raw_evidence=(),
        lever_directives_legacy=None,
        discovery_rationale="missing join",
        priority=1,
    )


def test_stage_2_for_skill_dispatches_l4(monkeypatch):
    """L4 dispatcher delegates to _call_llm_for_join_discovery and
    returns its list[{join_spec, rationale}] output unchanged."""
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    captured_calls = []
    def _fake_join_discovery(metadata_snapshot, hints, w=None, **kwargs):
        captured_calls.append({"hints": hints})
        return [{
            "join_spec": {
                "left_table": "catalog.schema.fact_bookings",
                "right_table": "catalog.schema.dim_hotel",
                "join_guidance": "fact_bookings.hotel_key = dim_hotel.hotel_key",
            },
            "rationale": "from L4 stub",
        }]
    monkeypatch.setattr(optimizer, "_call_llm_for_join_discovery", _fake_join_discovery)

    bundle = _sample_bundle("lever-4-join-discovery")
    result = three_stage_pipeline._stage_2_for_skill(bundle, w=None)

    assert result["skill_id"] == "lever-4-join-discovery"
    assert result["ag_id"] == "AG1"
    assert len(result["proposals"]) == 1
    assert result["proposals"][0]["join_spec"]["left_table"] == "catalog.schema.fact_bookings"
    assert len(captured_calls) == 1
    # hints derived from target_objects:
    assert any("fact_bookings" in str(h) for h in captured_calls[0]["hints"])


def test_stage_2_for_skill_unknown_skill_returns_empty(monkeypatch):
    """Unknown skill_id → empty proposals, error logged. Not an
    exception — the orchestrator continues with the rest of the AG's
    skill picks."""
    from genie_space_optimizer.optimization import three_stage_pipeline

    bundle = _sample_bundle("not-a-real-skill")
    result = three_stage_pipeline._stage_2_for_skill(bundle, w=None)
    assert result["proposals"] == []
    assert result["skill_id"] == "not-a-real-skill"
    assert "no adapter" in result.get("error", "").lower()


def test_stage_2_for_skill_records_capture_hit(monkeypatch):
    cfg = _reload_config_with_env({"GSO_THREE_STAGE_V1": "1"})
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    monkeypatch.setattr(optimizer, "_call_llm_for_join_discovery",
                         lambda metadata_snapshot, hints, w=None, **kwargs: [])

    bundle = _sample_bundle("lever-4-join-discovery")
    three_stage_pipeline._stage_2_for_skill(bundle, w=None)
    snap = cfg.dump_three_stage_capture_summary()
    assert snap["skill_dispatches"]["lever-4-join-discovery"] == 1


# ── Section 8: run_three_stage_pipeline_for_ag ────────────────────────


def test_pipeline_returns_stage_2_results_for_each_pick(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    monkeypatch.setattr(
        optimizer, "_call_llm_for_stage_1_discovery",
        lambda **kw: {
            "applicable_skills": [{
                "skill_id": "lever-4-join-discovery",
                "target_objects": ["catalog.schema.fact_bookings",
                                    "catalog.schema.dim_hotel"],
                "expected_impact_qids": ["Q1"],
                "evidence_refs": ["trace://q1"],
                "why": "missing join",
                "priority": 1,
            }],
            "discovery_rationale": "missing join across tables",
        },
    )
    monkeypatch.setattr(
        optimizer, "_call_llm_for_join_discovery",
        lambda metadata_snapshot, hints, w=None, **kwargs: [{
            "join_spec": {
                "left_table": "catalog.schema.fact_bookings",
                "right_table": "catalog.schema.dim_hotel",
                "join_guidance": "fact_bookings.hotel_key = dim_hotel.hotel_key",
            },
            "rationale": "from L4",
        }],
    )

    out = three_stage_pipeline.run_three_stage_pipeline_for_ag(
        ag_id="AG1",
        root_cause_summary="missing join",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    assert out["ag_id"] == "AG1"
    assert out["fallback_to_legacy"] is False
    assert len(out["stage_2_results"]) == 1
    assert out["stage_2_results"][0]["skill_id"] == "lever-4-join-discovery"
    assert len(out["stage_2_results"][0]["proposals"]) == 1


def test_pipeline_falls_back_to_legacy_on_empty_picks(monkeypatch):
    """Empty applicable_skills → fallback_to_legacy=True; no Stage-2
    calls; orchestrator returns the empty stage_2_results."""
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    monkeypatch.setattr(
        optimizer, "_call_llm_for_stage_1_discovery",
        lambda **kw: {"applicable_skills": [], "discovery_rationale": "nothing"},
    )

    out = three_stage_pipeline.run_three_stage_pipeline_for_ag(
        ag_id="AG1",
        root_cause_summary="x",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    assert out["fallback_to_legacy"] is True
    assert out["stage_2_results"] == []


def test_pipeline_merges_duplicate_skill_picks(monkeypatch):
    """Stage-1 returns two picks of the same skill_id with different
    target_objects. Pipeline merges them into one bundle and runs
    Stage-2 once."""
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    monkeypatch.setattr(
        optimizer, "_call_llm_for_stage_1_discovery",
        lambda **kw: {
            "applicable_skills": [
                {"skill_id": "lever-4-join-discovery",
                 "target_objects": [
                     "catalog.schema.fact_bookings",
                     "catalog.schema.dim_hotel",
                 ],
                 "expected_impact_qids": ["Q1"],
                 "evidence_refs": [], "why": "join1", "priority": 1},
                {"skill_id": "lever-4-join-discovery",
                 "target_objects": [
                     "catalog.schema.dim_hotel",
                     "catalog.schema.fact_bookings",
                 ],
                 "expected_impact_qids": ["Q2"],
                 "evidence_refs": [], "why": "join2", "priority": 1},
            ],
            "discovery_rationale": "two join hints",
        },
    )
    call_count = {"n": 0}
    def _fake_join_discovery(metadata_snapshot, hints, w=None, **kwargs):
        call_count["n"] += 1
        return []
    monkeypatch.setattr(optimizer, "_call_llm_for_join_discovery", _fake_join_discovery)

    out = three_stage_pipeline.run_three_stage_pipeline_for_ag(
        ag_id="AG1",
        root_cause_summary="x",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    # One Stage-2 call (merged), not two:
    assert call_count["n"] == 1
    assert len(out["stage_2_results"]) == 1


# ── Section 9: harness selector ───────────────────────────────────────


def test_select_strategy_path_pipeline_uses_three_stage(monkeypatch):
    cfg = _reload_config_with_env({})
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    legacy_calls = {"n": 0}
    pipeline_calls = {"n": 0}
    monkeypatch.setattr(
        optimizer, "_call_llm_for_adaptive_strategy",
        lambda **kw: (legacy_calls.__setitem__("n", legacy_calls["n"] + 1)
                       or {"action_groups": [{"id": "AG1"}]}),
    )
    monkeypatch.setattr(
        three_stage_pipeline, "run_three_stage_pipeline_for_ag",
        lambda **kw: (pipeline_calls.__setitem__("n", pipeline_calls["n"] + 1)
                      or {"ag_id": "AG1", "stage_2_results": [], "fallback_to_legacy": False}),
    )

    out = three_stage_pipeline._select_strategy_path_for_iteration(
        legacy_kwargs={
            "clusters": [], "soft_signal_clusters": [],
            "metadata_snapshot": _sample_metadata_snapshot(),
            "reflection_buffer": [], "priority_ranking": [],
            "tried_patches": set(), "w": None,
        },
        clusters_for_pipeline=[_sample_cluster()],
    )
    assert legacy_calls["n"] == 0
    assert pipeline_calls["n"] == 1
    assert out["source"] == "three_stage_pipeline"


def test_select_strategy_path_pipeline_fallback_runs_legacy(monkeypatch):
    """When pipeline returns fallback_to_legacy=True, the selector
    runs the legacy strategist after all and applies that result."""
    cfg = _reload_config_with_env({"GSO_THREE_STAGE_V1": "1"})
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    monkeypatch.setattr(
        optimizer, "_call_llm_for_adaptive_strategy",
        lambda **kw: {"action_groups": [{"id": "AG_LEGACY"}],
                       "global_instruction_rewrite": {"text": "fb-GIR"},
                       "rationale": "fb-rationale"},
    )
    monkeypatch.setattr(
        three_stage_pipeline, "run_three_stage_pipeline_for_ag",
        lambda **kw: {"ag_id": "AG1", "stage_2_results": [],
                       "fallback_to_legacy": True},
    )

    out = three_stage_pipeline._select_strategy_path_for_iteration(
        legacy_kwargs={
            "clusters": [], "soft_signal_clusters": [],
            "metadata_snapshot": _sample_metadata_snapshot(),
            "reflection_buffer": [], "priority_ranking": [],
            "tried_patches": set(), "w": None,
        },
        clusters_for_pipeline=[_sample_cluster()],
    )
    assert out["source"] == "legacy_strategist_after_fallback"
    assert out["legacy_action_groups"][0]["id"] == "AG_LEGACY"
    # Divergence: full legacy dict preserved on fallback so harness can
    # restore rationale + global_instruction_rewrite byte-stably.
    assert out["legacy_strategy_full"]["rationale"] == "fb-rationale"


# ── Section 9b: _emit_three_stage_shadow_comparison ───────────────────


def test_emit_shadow_comparison_records_overlap():
    cfg = _reload_config_with_env({})
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer

    optimizer._emit_three_stage_shadow_comparison(
        ag_id="AG1",
        stage_1_picks=[
            {"skill_id": "lever-4-join-discovery"},
            {"skill_id": "lever-1-table-column-description"},
        ],
        legacy_action_groups=[{
            "id": "AG1",
            "lever_directives": {"1": {"tables": []}, "4": {"join_specs": []}},
        }],
        pipeline_stage_2_results=[
            {"skill_id": "lever-4-join-discovery", "proposals": [{"x": 1}]},
            {"skill_id": "lever-1-table-column-description", "proposals": []},
        ],
    )
    snap = cfg.dump_three_stage_capture_summary()
    assert snap["shadow_comparisons"] == 1




def test_emit_shadow_comparison_includes_overlap_metrics():
    """Real comparison record must include skill_id sets + overlap
    metrics so the export script can compute fixture-level statistics."""
    import json
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "cap.ndjson"
        cfg = _reload_config_with_env({
            "GSO_THREE_STAGE_SHADOW_V1": "1",
            "GSO_THREE_STAGE_CAPTURE_PATH": str(path),
        })
        cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
        from genie_space_optimizer.optimization import optimizer

        optimizer._emit_three_stage_shadow_comparison(
            ag_id="AG1",
            stage_1_picks=[
                {"skill_id": "lever-4-join-discovery"},
                {"skill_id": "lever-1-table-column-description"},
            ],
            legacy_action_groups=[{
                "id": "AG1",
                "lever_directives": {"4": {"join_specs": []}, "5": {}},
            }],
            pipeline_stage_2_results=[
                {"skill_id": "lever-4-join-discovery", "proposals": []},
                {"skill_id": "lever-1-table-column-description", "proposals": []},
            ],
        )
        record = json.loads(path.read_text().strip())
        assert record["ag_id"] == "AG1"
        assert sorted(record["stage_1_skill_ids"]) == [
            "lever-1-table-column-description", "lever-4-join-discovery",
        ]
        assert sorted(record["legacy_lever_keys"]) == ["4", "5"]
        # Mapping legacy lever keys → skill_ids: {4} → lever-4-join-discovery,
        # {5} → lever-5a-instructions or lever-5b-example-sql (both, in
        # split-mode) — emitter records the mapped form for diff:
        assert "structural_overlap" in record
        assert 0.0 <= record["structural_overlap"] <= 1.0


def test_project_pipeline_to_action_groups_preserves_skill_to_lever_map():
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _project_pipeline_to_action_groups,
    )
    pipeline_result = {
        "ag_id": "AG1",
        "discovery_rationale": "missing join + missing instruction",
        "stage_2_results": [
            {"skill_id": "lever-4-join-discovery",
             "proposals": [{"join_spec": {"left_table": "t1"}}]},
            {"skill_id": "lever-5a-instructions",
             "proposals": [{"instruction_text": "PURPOSE:\nX"}]},
            {"skill_id": "lever-5b-example-sql",
             "proposals": [{"example_sql": "SELECT 1"}]},
        ],
    }
    ags = _project_pipeline_to_action_groups(pipeline_result)
    assert len(ags) == 1
    ag = ags[0]
    assert ag["id"] == "AG1"
    assert ag["_three_stage_pipeline"] is True
    assert "4" in ag["lever_directives"]
    assert "5" in ag["lever_directives"]
    # Both 5a and 5b proposals merged into key "5":
    five = ag["lever_directives"]["5"]["_pipeline_proposals"]
    assert len(five) == 2


# ── Section 7b: L1/L2 stage-2 adapters ────────────────────────────────


def test_stage_2_l1_produces_one_proposal_per_target(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    def _fake_call_llm_for_proposal(cluster, metadata_snapshot, patch_type, lever, w=None, **kwargs):
        return {
            "proposed_value": f"description for {patch_type}",
            "rationale": "ok",
            "_target_for_test": patch_type,
        }
    monkeypatch.setattr(optimizer, "_call_llm_for_proposal", _fake_call_llm_for_proposal)

    bundle = _sample_bundle("lever-1-table-column-description")
    out = three_stage_pipeline._stage_2_l1(bundle, w=None)
    assert out["skill_id"] == "lever-1-table-column-description"
    assert len(out["proposals"]) == len(bundle.target_objects)


def test_stage_2_l1_uses_add_table_description_for_table_targets(monkeypatch):
    """Targets without a column suffix → patch_type='add_table_description'.
    Targets with `<table>.<col>` → 'add_column_description'."""
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline
    from genie_space_optimizer.optimization.activation_bundle import ActivationBundle
    from genie_space_optimizer.optimization.afs import format_afs

    captured = []
    def _fake(cluster, metadata_snapshot, patch_type, lever, w=None, **kwargs):
        captured.append(patch_type)
        return {"proposed_value": "x", "rationale": "y"}
    monkeypatch.setattr(optimizer, "_call_llm_for_proposal", _fake)

    bundle = ActivationBundle(
        skill_id="lever-1-table-column-description",
        ag_id="AG1",
        target_objects=("catalog.schema.fact_bookings",
                        "catalog.schema.fact_bookings.booking_date"),
        cluster_afs=(format_afs(_sample_cluster()),),
        metadata_snapshot=_sample_metadata_snapshot(),
        identifier_allowlist="",
        evidence_refs=(), expected_impact_qids=(),
        raw_evidence=(), lever_directives_legacy=None,
        discovery_rationale="", priority=1,
    )
    three_stage_pipeline._stage_2_l1(bundle, w=None)
    assert "add_table_description" in captured
    assert "add_column_description" in captured


def test_stage_2_l2_passes_lever_2(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    captured_levers = []
    def _fake(cluster, metadata_snapshot, patch_type, lever, w=None):
        captured_levers.append(lever)
        return {"proposed_value": "x", "rationale": "y"}
    monkeypatch.setattr(optimizer, "_call_llm_for_proposal", _fake)

    bundle = _sample_bundle("lever-2-mv-column-refinement")
    three_stage_pipeline._stage_2_l2(bundle, w=None)
    assert all(lv == 2 for lv in captured_levers)


def test_stage_2_l1_dispatcher_table_registered():
    from genie_space_optimizer.optimization import three_stage_pipeline
    assert "lever-1-table-column-description" in three_stage_pipeline._STAGE_2_DISPATCH_TABLE
    assert "lever-2-mv-column-refinement" in three_stage_pipeline._STAGE_2_DISPATCH_TABLE


def test_stage_2_l3_uses_lever_3(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    captured = []
    def _fake(cluster, metadata_snapshot, patch_type, lever, w=None):
        captured.append((lever, patch_type))
        return {"proposed_value": "x", "rationale": "y"}
    monkeypatch.setattr(optimizer, "_call_llm_for_proposal", _fake)

    bundle = _sample_bundle("lever-3-tvf-routing")
    out = three_stage_pipeline._stage_2_l3(bundle, w=None)
    assert out["skill_id"] == "lever-3-tvf-routing"
    assert all(lv == 3 for lv, _ in captured)
    assert all(pt == "add_tvf_description" for _, pt in captured)


def test_stage_2_l3_dispatcher_table_registered():
    from genie_space_optimizer.optimization import three_stage_pipeline
    assert "lever-3-tvf-routing" in three_stage_pipeline._STAGE_2_DISPATCH_TABLE


# ── Section 7c: L5a + L5b stage-2 adapters ────────────────────────────


def test_stage_2_l5a_returns_instruction_text_only(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    monkeypatch.setattr(
        optimizer, "_call_llm_for_lever_5a_instructions",
        lambda **kw: {"instruction_text": "PURPOSE:\nX", "rationale": "r5a"},
    )

    bundle = _sample_bundle("lever-5a-instructions")
    out = three_stage_pipeline._stage_2_l5a(bundle, w=None)
    assert out["skill_id"] == "lever-5a-instructions"
    assert len(out["proposals"]) == 1
    assert out["proposals"][0]["instruction_text"] == "PURPOSE:\nX"
    assert "example_sql" not in out["proposals"][0]


def test_stage_2_l5b_runs_once_per_cluster(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    call_count = {"n": 0}
    def _fake_5b(cluster, metadata_snapshot, w, benchmark_corpus):
        call_count["n"] += 1
        return [{"example_question": "Q?", "example_sql": "SELECT 1",
                 "parameters": [], "usage_guidance": "test"}]
    monkeypatch.setattr(optimizer, "_dispatch_lever_5b_for_cluster", _fake_5b)

    # Bundle with two clusters:
    from genie_space_optimizer.optimization.activation_bundle import ActivationBundle
    from genie_space_optimizer.optimization.afs import format_afs
    bundle = ActivationBundle(
        skill_id="lever-5b-example-sql",
        ag_id="AG1", target_objects=(),
        cluster_afs=(format_afs(_sample_cluster()),
                      format_afs({**_sample_cluster(), "cluster_id": "C2"})),
        metadata_snapshot=_sample_metadata_snapshot(),
        identifier_allowlist="", evidence_refs=(),
        expected_impact_qids=(), raw_evidence=(),
        lever_directives_legacy=None, discovery_rationale="",
        priority=1,
    )
    out = three_stage_pipeline._stage_2_l5b(bundle, w=None)
    assert call_count["n"] == 2
    assert len(out["proposals"]) == 2


def test_stage_2_l5a_l5b_dispatcher_registered():
    from genie_space_optimizer.optimization import three_stage_pipeline
    assert "lever-5a-instructions" in three_stage_pipeline._STAGE_2_DISPATCH_TABLE
    assert "lever-5b-example-sql" in three_stage_pipeline._STAGE_2_DISPATCH_TABLE


# ── Section 7d: L6 stage-2 adapter ────────────────────────────────────


def test_stage_2_l6_returns_proposal_per_cluster(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    def _fake_l6(cluster, metadata_snapshot, *, strategist_hints=None,
                 w=None, spark=None, catalog="", gold_schema="",
                 warehouse_id="", benchmarks=None, **kwargs):
        # Must be a canonical proposal: contract-first hardening
        # requires patch_type + target on every Stage-2 output.
        return {"snippet_type": "filter", "sql": "x = 1",
                 "instruction": "for X questions",
                 "patch_type": "add_sql_snippet_filter",
                 "target": "mv.fact.x"}
    monkeypatch.setattr(optimizer, "_generate_lever6_proposal", _fake_l6)

    bundle = _sample_bundle("lever-6-sql-expression")
    out = three_stage_pipeline._stage_2_l6(bundle, w=None)
    assert out["skill_id"] == "lever-6-sql-expression"
    assert len(out["proposals"]) == 1
    assert out["proposals"][0]["snippet_type"] == "filter"


def test_stage_2_l6_drops_none_proposals(monkeypatch):
    """_generate_lever6_proposal returns None on validation failure;
    adapter must filter Nones."""
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    monkeypatch.setattr(
        optimizer, "_generate_lever6_proposal",
        lambda *a, **kw: None,
    )
    bundle = _sample_bundle("lever-6-sql-expression")
    out = three_stage_pipeline._stage_2_l6(bundle, w=None)
    assert out["proposals"] == []


def test_stage_2_l6_dispatcher_registered():
    from genie_space_optimizer.optimization import three_stage_pipeline
    assert "lever-6-sql-expression" in three_stage_pipeline._STAGE_2_DISPATCH_TABLE


# ── Section 9c: harness summary ───────────────────────────────────────


def test_dump_three_stage_capture_summary_safe_when_no_state():
    from genie_space_optimizer.common import config as cfg
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    snap = cfg.dump_three_stage_capture_summary()
    assert isinstance(snap, dict)
    assert snap["discovery_calls"] == 0
    assert all(c == 0 for c in snap["skill_dispatches"].values())


# ── Section: Rich skill_catalogue renderer ────────────────────────────


def test_render_rich_skill_catalogue_includes_description_and_when_to_pick():
    """Each line in the rendered catalogue must carry the skill_id,
    the description, and the when_to_pick guidance pulled from
    SKILL.md frontmatter."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_rich_skill_catalogue,
    )
    rendered = _render_rich_skill_catalogue()
    assert "lever-4-join-discovery" in rendered
    assert "join_specs" in rendered, (
        "lever-4 description must mention join_specs"
    )
    assert "Pick when:" in rendered, (
        "every entry must carry a 'Pick when:' line"
    )
    assert "What:" in rendered, (
        "every entry must carry a 'What:' line"
    )


def test_render_rich_skill_catalogue_emits_one_block_per_pickable_skill():
    from genie_space_optimizer.common.config import _THREE_STAGE_SKILL_NAMES
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_rich_skill_catalogue,
    )
    rendered = _render_rich_skill_catalogue()
    for sid in _THREE_STAGE_SKILL_NAMES:
        assert sid in rendered, f"skill {sid} missing from rich catalogue"


def test_render_rich_skill_catalogue_is_deterministic_across_calls():
    """Two back-to-back calls return byte-identical output. The loader
    caches in-process; this guards against accidental ordering or
    formatting non-determinism in the renderer itself."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_rich_skill_catalogue,
    )
    a = _render_rich_skill_catalogue()
    b = _render_rich_skill_catalogue()
    assert a == b


def test_render_rich_skill_catalogue_falls_back_to_bare_id_when_metadata_missing(
    tmp_path,
):
    """If a skill's SKILL.md is missing description/when_to_pick (e.g.
    a new skill landed without updating frontmatter), the renderer
    MUST emit a bare-ID bullet for that skill instead of raising.
    Stage-1 is more robust with bare-IDs-for-some than with a hard
    crash."""
    from genie_space_optimizer.skills._loader import SkillLoader
    from genie_space_optimizer.optimization import three_stage_pipeline
    skill_dir = tmp_path / "lever-x-no-meta"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text(
        "---\n"
        "skill_id: lever-x-no-meta\n"
        "prompt_constant_name: NOT_USED\n"
        "causal_or_non_causal: causal\n"
        "pickable_by_stage_1: true\n"
        "---\n"
        "body\n",
        encoding="utf-8",
    )
    test_loader = SkillLoader(root=tmp_path)
    rendered = three_stage_pipeline._render_rich_skill_catalogue(
        skill_ids=("lever-x-no-meta",),
        loader=test_loader,
    )
    assert rendered == "- lever-x-no-meta", (
        f"missing-metadata fallback must be bare-id bullet, got: {rendered!r}"
    )


def test_stage_1_discovery_prompt_render_includes_rich_catalogue_via_helper():
    """End-to-end: format the actual STAGE_1_DISCOVERY_PROMPT with the
    helper's output and assert the rendered prompt carries 'Pick when:'
    for every pickable skill. This is the gate that catches a future
    regression where someone bypasses the helper."""
    from genie_space_optimizer.common.config import (
        STAGE_1_DISCOVERY_PROMPT,
        _THREE_STAGE_SKILL_NAMES,
        format_mlflow_template,
    )
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_failure_type_routing_table,
        _render_rich_skill_catalogue,
    )
    rendered = format_mlflow_template(
        STAGE_1_DISCOVERY_PROMPT,
        space_description="Hotel bookings analytics",
        ag_id="AG_TEST",
        root_cause_summary="missing join between fact_bookings and dim_hotel",
        cluster_briefs="C1: missing_join — hotel_key not joined to dim_hotel",
        skill_catalogue=_render_rich_skill_catalogue(),
        failure_type_routing_table=_render_failure_type_routing_table(),
        identifier_allowlist="catalog.schema.fact_bookings.hotel_key",
    )
    assert "{{" not in rendered, "unrendered template variable"
    for sid in _THREE_STAGE_SKILL_NAMES:
        assert sid in rendered, f"skill {sid} missing from rendered prompt"
    # Count the number of 'Pick when:' substrings — must equal the
    # number of pickable skills:
    assert rendered.count("Pick when:") == len(_THREE_STAGE_SKILL_NAMES), (
        f"expected {len(_THREE_STAGE_SKILL_NAMES)} 'Pick when:' lines, "
        f"got {rendered.count('Pick when:')}"
    )
    assert rendered.count("What:") == len(_THREE_STAGE_SKILL_NAMES), (
        f"expected {len(_THREE_STAGE_SKILL_NAMES)} 'What:' lines, "
        f"got {rendered.count('What:')}"
    )


def test_stage_1_caller_uses_rich_catalogue_helper(monkeypatch):
    """Patch _render_rich_skill_catalogue to a sentinel; assert
    _call_llm_for_stage_1_discovery surfaces the sentinel in the
    rendered prompt. This catches a regression where someone
    re-inlines the bare-id joiner at the call site.

    Why patch the three_stage_pipeline module (not optimizer): the
    Task 3 wire-in uses a function-local `from ... import
    _render_rich_skill_catalogue` which re-reads the module
    namespace on every call. Patching ts_mod's attribute therefore
    takes effect on the next invocation. Same pattern applies to
    _link_prompt_to_trace, which is function-local-imported from
    evaluation.
    """
    from genie_space_optimizer.optimization import optimizer as opt_mod
    from genie_space_optimizer.optimization import three_stage_pipeline as ts_mod
    from genie_space_optimizer.optimization import evaluation as eval_mod

    SENTINEL = "RICH_CATALOGUE_SENTINEL_XYZ_8675309"

    monkeypatch.setattr(
        ts_mod, "_render_rich_skill_catalogue",
        lambda *a, **kw: SENTINEL,
    )

    captured: dict[str, str] = {}

    def _fake_call_llm_openai(w, system_msg, prompt, **kwargs):
        # After 2026-05-17-active-callsite-typed-output-wiring Task 8,
        # Stage-1 routes through _traced_llm_call(w, system_msg, prompt, ...)
        captured["prompt"] = prompt
        return ('{"applicable_skills": [], "discovery_rationale": "stub"}', None)

    monkeypatch.setattr(opt_mod, "_traced_llm_call", _fake_call_llm_openai)
    # _link_prompt_to_trace is function-local-imported from
    # evaluation; patch it on its origin module:
    monkeypatch.setattr(eval_mod, "_link_prompt_to_trace", lambda *a, **kw: None)

    result = opt_mod._call_llm_for_stage_1_discovery(
        ag_id="AG_T",
        root_cause_summary="test",
        clusters=[],
        metadata_snapshot={"config": {"description": "Test space"}},
        w=None,
    )
    assert isinstance(result, dict)
    assert SENTINEL in captured.get("prompt", ""), (
        "_call_llm_for_stage_1_discovery did not route through "
        "_render_rich_skill_catalogue — check the wire-in at "
        "optimizer.py:~10883"
    )


# ── Section: Stage-1 rendered-prompt byte-stability snapshot ──────────


_STAGE_1_SNAPSHOT_PATH = (
    __import__("pathlib").Path(__file__).resolve().parents[3]
    / "tests" / "fixtures" / "three_stage_v1" / "stage_1_prompt_snapshot.txt"
)

_STAGE_1_SNAPSHOT_KWARGS = {
    "space_description": "Hotel bookings analytics for revenue management.",
    "ag_id": "AG_SNAPSHOT_FIXED",
    "root_cause_summary": "missing join between fact_bookings and dim_hotel; ambiguous channel filter",
    "cluster_briefs": (
        "C1 (missing_join, 3 qids): hotel_key on fact_bookings not joined to dim_hotel\n"
        "C2 (wrong_filter, 2 qids): channel='direct' applied but channel column is on dim_distribution"
    ),
    "identifier_allowlist": (
        "catalog.schema.fact_bookings, catalog.schema.dim_hotel, "
        "catalog.schema.dim_distribution"
    ),
}


def test_stage_1_rendered_prompt_snapshot_byte_stable():
    """Renders STAGE_1_DISCOVERY_PROMPT with fixed kwargs and the live
    rich catalogue, then compares byte-for-byte against the committed
    snapshot. Drift is caught immediately.

    To regenerate after an intentional prompt or frontmatter change:
        python -c 'from tests.unit.optimization.test_three_stage_v1 \
            import _regen_stage_1_snapshot; _regen_stage_1_snapshot()'
    """
    from genie_space_optimizer.common.config import (
        STAGE_1_DISCOVERY_PROMPT,
        format_mlflow_template,
    )
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_failure_type_routing_table,
        _render_rich_skill_catalogue,
    )

    rendered = format_mlflow_template(
        STAGE_1_DISCOVERY_PROMPT,
        skill_catalogue=_render_rich_skill_catalogue(),
        failure_type_routing_table=_render_failure_type_routing_table(),
        **_STAGE_1_SNAPSHOT_KWARGS,
    )

    assert _STAGE_1_SNAPSHOT_PATH.is_file(), (
        f"missing snapshot: {_STAGE_1_SNAPSHOT_PATH}. "
        "Run _regen_stage_1_snapshot() to create it."
    )
    expected = _STAGE_1_SNAPSHOT_PATH.read_text(encoding="utf-8")
    assert rendered == expected, (
        "Rendered Stage-1 prompt drifted from snapshot. If this change "
        "is intentional, regenerate the snapshot:\n"
        "  python -c 'from tests.unit.optimization.test_three_stage_v1 "
        "import _regen_stage_1_snapshot; _regen_stage_1_snapshot()'\n"
        "Then review the diff carefully — every Stage-1 LLM call will "
        "see the new bytes."
    )


def _regen_stage_1_snapshot() -> None:
    """Regenerate the committed snapshot. Manual invocation only;
    NOT a pytest test. Call this when:
      * a pickable skill's description/when_to_pick changes,
      * STAGE_1_DISCOVERY_PROMPT template body changes,
      * _render_rich_skill_catalogue formatting changes,
      * _THREE_STAGE_SKILL_NAMES gains or loses a skill.
    Always inspect the resulting diff with `git diff --stat` before
    committing.
    """
    from genie_space_optimizer.common.config import (
        STAGE_1_DISCOVERY_PROMPT,
        format_mlflow_template,
    )
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_failure_type_routing_table,
        _render_rich_skill_catalogue,
    )
    rendered = format_mlflow_template(
        STAGE_1_DISCOVERY_PROMPT,
        skill_catalogue=_render_rich_skill_catalogue(),
        failure_type_routing_table=_render_failure_type_routing_table(),
        **_STAGE_1_SNAPSHOT_KWARGS,
    )
    _STAGE_1_SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STAGE_1_SNAPSHOT_PATH.write_text(rendered, encoding="utf-8")
    print(f"wrote {len(rendered)} bytes to {_STAGE_1_SNAPSHOT_PATH}")


# ── Section: Stage-1 prompt hardening (2026-05-17) ────────────────────


def test_format_cluster_briefs_afs_includes_question_ids():
    """The Stage-1 cluster briefs must surface a 'Question IDs:' line
    per hard cluster so Stage-1 can ground expected_impact_qids in a
    real choice set."""
    from genie_space_optimizer.optimization.optimizer import (
        _format_cluster_briefs_afs,
    )
    clusters = [
        {
            "cluster_id": "C001",
            "root_cause": "missing_join",
            "asi_blame_set": ["cat.sch.fact_orders", "cat.sch.dim_product"],
            "affected_judge": "schema_accuracy",
            "question_ids": ["Q12", "Q14", "Q19"],
            "signal_type": "hard",
        },
    ]
    rendered = _format_cluster_briefs_afs(clusters, top_n=5)
    assert "Question IDs: Q12, Q14, Q19" in rendered, (
        f"expected 'Question IDs:' line in briefs; got:\n{rendered}"
    )


def test_format_cluster_briefs_afs_truncates_long_qid_lists():
    """Long qid lists must be truncated in the briefs (the full list
    is in AFS; the brief gets a preview with '+N more' suffix)."""
    from genie_space_optimizer.optimization.optimizer import (
        _format_cluster_briefs_afs,
    )
    clusters = [
        {
            "cluster_id": "C001",
            "root_cause": "missing_join",
            "asi_blame_set": ["cat.sch.fact_orders"],
            "affected_judge": "schema_accuracy",
            "question_ids": [f"Q{i}" for i in range(1, 21)],  # 20 qids
            "signal_type": "hard",
        },
    ]
    rendered = _format_cluster_briefs_afs(clusters, top_n=5)
    assert "Question IDs: Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, +5 more" in rendered, (
        f"expected first 10 qids + '+5 more'; got:\n{rendered}"
    )


def test_format_cluster_briefs_afs_omits_qids_line_when_empty():
    """Empty question_ids must not render an empty 'Question IDs:'
    line in the brief."""
    from genie_space_optimizer.optimization.optimizer import (
        _format_cluster_briefs_afs,
    )
    clusters = [
        {
            "cluster_id": "C001",
            "root_cause": "missing_join",
            "asi_blame_set": ["cat.sch.fact_orders"],
            "affected_judge": "schema_accuracy",
            "question_ids": [],
            "signal_type": "hard",
        },
    ]
    rendered = _format_cluster_briefs_afs(clusters, top_n=5)
    assert "Question IDs:" not in rendered


# ── Section: Failure-type routing table (Task 2) ──────────────────────


def test_render_failure_type_routing_table_covers_every_lever_map_entry():
    """Every entry in _ROOT_CAUSE_LEVER_MAP with lever != 0 must
    appear in the rendered routing table, mapped to its skill_id(s).
    Lever=0 entries (extra_columns_only, select_star) are intentionally
    omitted — they route to no skill."""
    from genie_space_optimizer.optimization.optimizer import (
        _ROOT_CAUSE_LEVER_MAP,
    )
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_failure_type_routing_table,
    )
    rendered = _render_failure_type_routing_table()
    for failure_type, lever in _ROOT_CAUSE_LEVER_MAP.items():
        if lever == 0:
            assert failure_type not in rendered, (
                f"{failure_type} (lever=0) must be omitted from table"
            )
            continue
        assert failure_type in rendered, (
            f"{failure_type} (lever={lever}) missing from routing table"
        )


def test_render_failure_type_routing_table_maps_lever_5_to_both_5a_and_5b():
    """Lever 5 ambiguously routes to lever-5a (instructions) or
    lever-5b (example_sql). The rendered table must show both options
    so the model can decompose based on cluster shape."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_failure_type_routing_table,
    )
    rendered = _render_failure_type_routing_table()
    # missing_instruction -> lever 5 -> 5a or 5b
    instruction_line = next(
        (line for line in rendered.splitlines()
         if "missing_instruction" in line),
        None,
    )
    assert instruction_line is not None
    assert "lever-5a-instructions" in instruction_line
    assert "lever-5b-example-sql" in instruction_line


def test_render_failure_type_routing_table_uses_pipe_table_format():
    """Output must be a Markdown pipe table so Claude parses it
    unambiguously. Header row + separator row + data rows."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_failure_type_routing_table,
    )
    rendered = _render_failure_type_routing_table()
    lines = rendered.splitlines()
    assert lines[0].startswith("| failure_type"), (
        f"first line must be table header; got: {lines[0]!r}"
    )
    assert lines[1].startswith("|---"), (
        f"second line must be table separator; got: {lines[1]!r}"
    )


def test_stage_1_prompt_includes_failure_type_routing_table_slot():
    """The Stage-1 prompt template must expose a
    {{ failure_type_routing_table }} slot in the <context> block."""
    from genie_space_optimizer.common.config import STAGE_1_DISCOVERY_PROMPT
    assert "{{ failure_type_routing_table }}" in STAGE_1_DISCOVERY_PROMPT, (
        "STAGE_1_DISCOVERY_PROMPT must expose "
        "{{ failure_type_routing_table }} slot"
    )


def test_call_llm_for_stage_1_discovery_passes_routing_table_kwarg(monkeypatch):
    """_call_llm_for_stage_1_discovery must populate the
    failure_type_routing_table format_kwarg."""
    from genie_space_optimizer.optimization import optimizer
    captured_prompt: dict = {}

    def _fake_llm_openai(w, system_msg, prompt, **kwargs):
        captured_prompt["text"] = prompt
        return ('{"applicable_skills": [], "discovery_rationale": ""}', None)
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="missing join",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    assert "text" in captured_prompt, "user prompt was not captured"
    assert "missing_join" in captured_prompt["text"], (
        "routing table must be rendered into the prompt"
    )
    assert "lever-4-join-discovery" in captured_prompt["text"], (
        "routing table must include skill_ids"
    )


# ── Section: <how_to_read_cluster_briefs> (Task 3) ────────────────────


def test_stage_1_prompt_includes_how_to_read_cluster_briefs_section():
    """The Stage-1 prompt must explain which AFS field feeds which
    output slot. Without this section the model has to guess (e.g.
    'should suggested_fix_summary go in why or rationale?')."""
    from genie_space_optimizer.common.config import STAGE_1_DISCOVERY_PROMPT
    assert "<how_to_read_cluster_briefs>" in STAGE_1_DISCOVERY_PROMPT
    # Each AFS field must be explicitly mapped to an output slot.
    for needle in [
        "failure_type",
        "Blamed objects",
        "Suggested fixes",
        "Question IDs",
        "Judge verdict pattern",
    ]:
        assert needle in STAGE_1_DISCOVERY_PROMPT, (
            f"missing AFS field {needle!r} in how_to_read_cluster_briefs"
        )
    # Each output slot must be referenced.
    for needle in [
        "skill_id",
        "target_objects",
        "why",
        "expected_impact_qids",
        "priority",
    ]:
        assert needle in STAGE_1_DISCOVERY_PROMPT, (
            f"missing output slot {needle!r} in how_to_read_cluster_briefs"
        )


# ── Section: <routing_examples> (Task 4) ──────────────────────────────


def test_stage_1_prompt_includes_routing_examples_covering_decision_boundaries():
    """The Stage-1 prompt must include canonical worked examples
    covering distinct decision boundaries: single-defect, counterintuitive
    route, compound failure, no-fit empty pick, soft-cluster skip."""
    from genie_space_optimizer.common.config import STAGE_1_DISCOVERY_PROMPT
    assert "<routing_examples>" in STAGE_1_DISCOVERY_PROMPT
    # Five boundary cases, identified by unique anchor strings.
    for needle in [
        "### Example 1: Single-defect cluster",
        "### Example 2: Counterintuitive route",
        "### Example 3: Compound failure",
        "### Example 4: No-fit cluster",
        "### Example 5: Soft cluster only",
    ]:
        assert needle in STAGE_1_DISCOVERY_PROMPT, (
            f"missing routing example: {needle!r}"
        )
    # Example 2 must cover wrong_aggregation -> lever-6 (NOT lever-2)
    # to reinforce the Task 2 routing table's counterintuitive entry.
    assert "wrong_aggregation" in STAGE_1_DISCOVERY_PROMPT
    assert "lever-6-sql-expression" in STAGE_1_DISCOVERY_PROMPT


def test_stage_1_prompt_routing_examples_each_include_json_output():
    """Each example must include a concrete JSON output block matching
    the output_schema, so the model has a complete worked sample."""
    from genie_space_optimizer.common.config import STAGE_1_DISCOVERY_PROMPT
    routing_block_start = STAGE_1_DISCOVERY_PROMPT.find("<routing_examples>")
    routing_block_end = STAGE_1_DISCOVERY_PROMPT.find("</routing_examples>")
    assert routing_block_start != -1 and routing_block_end != -1
    block = STAGE_1_DISCOVERY_PROMPT[routing_block_start:routing_block_end]
    # Each example's JSON output begins with '"applicable_skills":'
    n_outputs = block.count('"applicable_skills":')
    assert n_outputs >= 5, (
        f"expected >=5 JSON output blocks in <routing_examples>; "
        f"got {n_outputs}"
    )


# ── Section: Allowlist pre-filter (Task 5) ────────────────────────────


def _allowlist_metadata_snapshot_with_joins():
    return {
        "config": {"description": "test"},
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.fact_orders",
                    "column_configs": [
                        {"column_name": "order_id", "data_type": "STRING"},
                        {"column_name": "customer_id", "data_type": "STRING"},
                    ],
                    "join_specs": [
                        {"sql": ["fact_orders.customer_id = dim_customer.customer_id"]},
                    ],
                },
                {
                    "identifier": "cat.sch.dim_customer",
                    "column_configs": [
                        {"column_name": "customer_id", "data_type": "STRING"},
                        {"column_name": "region", "data_type": "STRING"},
                    ],
                    "join_specs": [],
                },
                {
                    "identifier": "cat.sch.dim_product",
                    "column_configs": [
                        {"column_name": "product_id", "data_type": "STRING"},
                    ],
                    "join_specs": [],
                },
                {
                    "identifier": "cat.sch.fact_unrelated",
                    "column_configs": [
                        {"column_name": "thing_id", "data_type": "STRING"},
                    ],
                    "join_specs": [],
                },
            ],
            "metric_views": [],
            "functions": [],
        },
    }


def test_build_identifier_allowlist_full_preserves_legacy_behavior():
    """Calling _build_identifier_allowlist WITHOUT relevant_objects
    must return the full allowlist exactly as before."""
    from genie_space_optimizer.optimization.optimizer import (
        _build_identifier_allowlist,
    )
    snap = _allowlist_metadata_snapshot_with_joins()
    full = _build_identifier_allowlist(snap)
    assert "fact_orders" in full["tables_short"]
    assert "dim_customer" in full["tables_short"]
    assert "dim_product" in full["tables_short"]
    assert "fact_unrelated" in full["tables_short"]


def test_build_identifier_allowlist_filters_to_relevant_plus_1hop():
    """When relevant_objects is provided, the returned allowlist must
    contain only those identifiers + their 1-hop joined neighbors."""
    from genie_space_optimizer.optimization.optimizer import (
        _build_identifier_allowlist,
    )
    snap = _allowlist_metadata_snapshot_with_joins()
    # fact_orders is in blame; dim_customer is its 1-hop neighbor.
    # dim_product and fact_unrelated are NOT in blame and NOT 1-hop.
    filtered = _build_identifier_allowlist(
        snap, relevant_objects={"cat.sch.fact_orders"},
    )
    assert "fact_orders" in filtered["tables_short"]
    assert "dim_customer" in filtered["tables_short"], (
        "1-hop join neighbor must be included"
    )
    assert "dim_product" not in filtered["tables_short"], (
        "unrelated table must be filtered out"
    )
    assert "fact_unrelated" not in filtered["tables_short"]


def test_build_identifier_allowlist_empty_relevant_falls_back_to_full():
    """An empty relevant_objects set must fall back to the full
    allowlist (graceful degradation when blame extraction fails)."""
    from genie_space_optimizer.optimization.optimizer import (
        _build_identifier_allowlist,
    )
    snap = _allowlist_metadata_snapshot_with_joins()
    filtered = _build_identifier_allowlist(snap, relevant_objects=set())
    assert "fact_unrelated" in filtered["tables_short"], (
        "empty relevant_objects must fall back to full allowlist"
    )


def test_call_llm_for_stage_1_discovery_filters_allowlist_to_blamed_objects(monkeypatch):
    """End-to-end: Stage-1 must build a filtered allowlist from the
    cluster's blame_set and pass it to format_kwargs."""
    from genie_space_optimizer.optimization import optimizer
    captured: dict = {}

    def _fake_llm_openai(w, system_msg, prompt, **kwargs):
        # After 2026-05-17-active-callsite-typed-output-wiring Task 8,
        # Stage-1 calls _traced_llm_call(w, system_msg, prompt, ...).
        captured["text"] = prompt
        return ('{"applicable_skills": [], "discovery_rationale": ""}', None)
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    snap = _allowlist_metadata_snapshot_with_joins()
    cluster = {
        "cluster_id": "C001",
        "root_cause": "missing_join",
        "asi_blame_set": ["cat.sch.fact_orders"],
        "affected_judge": "schema_accuracy",
        "question_ids": ["Q1"],
        "signal_type": "hard",
    }
    optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="missing join",
        clusters=[cluster],
        metadata_snapshot=snap,
        w=None,
    )
    prompt = captured.get("text", "")
    assert "fact_orders" in prompt
    assert "dim_customer" in prompt, (
        "1-hop join neighbor must reach the prompt"
    )
    assert "fact_unrelated" not in prompt, (
        "unrelated table must be filtered out of the prompt"
    )


def test_call_llm_for_stage_1_discovery_full_allowlist_when_env_flag_on(monkeypatch):
    """Setting GSO_STAGE_1_ALLOWLIST_FULL=1 must bypass the filter
    (debugging escape hatch)."""
    from genie_space_optimizer.optimization import optimizer
    monkeypatch.setenv("GSO_STAGE_1_ALLOWLIST_FULL", "1")
    captured: dict = {}

    def _fake_llm_openai(w, system_msg, prompt, **kwargs):
        captured["text"] = prompt
        return ('{"applicable_skills": [], "discovery_rationale": ""}', None)
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    snap = _allowlist_metadata_snapshot_with_joins()
    cluster = {
        "cluster_id": "C001",
        "root_cause": "missing_join",
        "asi_blame_set": ["cat.sch.fact_orders"],
        "affected_judge": "schema_accuracy",
        "question_ids": ["Q1"],
        "signal_type": "hard",
    }
    optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="missing join",
        clusters=[cluster],
        metadata_snapshot=snap,
        w=None,
    )
    prompt = captured.get("text", "")
    assert "fact_unrelated" in prompt, (
        "GSO_STAGE_1_ALLOWLIST_FULL=1 must include unrelated tables"
    )


# ── Section: Stage-1 max_tokens (Task 6) ──────────────────────────────


def test_stage_1_discovery_passes_max_tokens_to_llm(monkeypatch):
    """The Stage-1 LLM call must pass an explicit max_tokens=2500
    (sized from Trial-5 stretch-case analysis: max observed compact
    response ~850 tokens, pretty-printed ~1170, 2x headroom = 2500).
    Databricks API best practice requires explicit max_tokens for
    OTPM reservation predictability."""
    from genie_space_optimizer.common.config import (
        STAGE_1_DISCOVERY_MAX_TOKENS,
    )
    assert STAGE_1_DISCOVERY_MAX_TOKENS == 2500, (
        "STAGE_1_DISCOVERY_MAX_TOKENS must be 2500 per evidence-based "
        "sizing analysis"
    )

    from genie_space_optimizer.optimization import optimizer
    captured_kwargs: dict = {}

    def _fake_llm_openai(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return ('{"applicable_skills": [], "discovery_rationale": ""}', None)
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="missing join",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )
    assert captured_kwargs.get("max_tokens") == 2500, (
        f"Stage-1 LLM call must pass max_tokens=2500; got "
        f"max_tokens={captured_kwargs.get('max_tokens')!r}"
    )


# ── Section: Targets line in skill catalogue (Task 2) ─────────────────


def test_render_rich_skill_catalogue_emits_targets_line_per_skill():
    """Each pickable skill block must carry a 'Targets:' line that
    surfaces target_kind and target_min_count from frontmatter, so the
    Stage-1 LLM sees the target-shape constraint at decision time."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_rich_skill_catalogue,
    )
    rendered = _render_rich_skill_catalogue()
    # lever-4 has the strongest constraint (min 2 base tables); use it
    # as the canary case.
    assert "    Targets: 2+ base_table" in rendered, (
        f"lever-4 'Targets:' line missing or malformed; got:\n{rendered}"
    )
    # Every pickable skill must have a Targets: line.
    n_targets_lines = rendered.count("    Targets:")
    n_what_lines = rendered.count("    What:")
    assert n_targets_lines == n_what_lines, (
        f"expected one 'Targets:' line per 'What:' line; "
        f"got {n_targets_lines} vs {n_what_lines}"
    )


def test_render_rich_skill_catalogue_targets_line_handles_empty_min_count():
    """target_min_count: 0 must render as 'any base_table' (or similar) —
    not '0+ base_table' which reads as nonsensical to the LLM."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_rich_skill_catalogue,
    )
    rendered = _render_rich_skill_catalogue()
    assert "0+ base_table" not in rendered, (
        "min_count=0 must not render as '0+'; got:\n" + rendered
    )
    # lever-1 has target_kind=base_table, target_min_count=0 → expect
    # 'any base_table' format.
    assert "    Targets: any base_table" in rendered, (
        f"lever-1 'Targets:' line malformed; got:\n{rendered}"
    )


def test_render_rich_skill_catalogue_targets_line_for_mixed_kind():
    """target_kind=mixed must render with a hint that any of
    table/MV/function is acceptable."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _render_rich_skill_catalogue,
    )
    rendered = _render_rich_skill_catalogue()
    # lever-5a-instructions has target_kind=mixed, target_min_count=0
    assert "    Targets: any table or metric_view (AG-wide allowed)" in rendered, (
        f"lever-5a 'Targets:' line missing or malformed; got:\n{rendered}"
    )


# ── Section: _coerce_target_objects_for_skill (Task 4) ────────────────


def _sample_allowlist():
    return {
        "tables": [
            "cat.sch.fact_bookings",
            "cat.sch.dim_hotel",
            "cat.sch.dim_distribution",
        ],
        "tables_short": {"fact_bookings", "dim_hotel", "dim_distribution"},
        "columns": {},
        "columns_flat": set(),
        "functions": ["cat.sch.tvf_revenue_by_channel"],
        "functions_short": {"tvf_revenue_by_channel"},
        "metric_views": ["cat.sch.mv_revenue_daily"],
    }


def test_coerce_filters_base_table_kind_against_tables_bucket():
    """target_kind='base_table' must drop targets that are not in
    allowlist['tables']. Stops Stage-1 from picking lever-1 with an
    MV FQN or a TVF FQN."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _coerce_target_objects_for_skill,
    )
    coerced, dropped = _coerce_target_objects_for_skill(
        skill_id="lever-1-table-column-description",
        target_kind="base_table",
        target_min_count=0,
        raw_targets=[
            "cat.sch.fact_bookings",       # valid base table
            "cat.sch.mv_revenue_daily",    # WRONG — MV, not base table
            "cat.sch.tvf_revenue_by_channel",  # WRONG — TVF
        ],
        allowlist=_sample_allowlist(),
    )
    assert coerced == ["cat.sch.fact_bookings"]
    assert dropped == [
        "cat.sch.mv_revenue_daily",
        "cat.sch.tvf_revenue_by_channel",
    ]


def test_coerce_filters_metric_view_kind_against_metric_views_bucket():
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _coerce_target_objects_for_skill,
    )
    coerced, dropped = _coerce_target_objects_for_skill(
        skill_id="lever-2-mv-column-refinement",
        target_kind="metric_view",
        target_min_count=0,
        raw_targets=[
            "cat.sch.mv_revenue_daily",
            "cat.sch.fact_bookings",  # WRONG — base table, not MV
        ],
        allowlist=_sample_allowlist(),
    )
    assert coerced == ["cat.sch.mv_revenue_daily"]
    assert dropped == ["cat.sch.fact_bookings"]


def test_coerce_drops_pick_below_min_count():
    """When post-filter count falls below target_min_count, the helper
    signals 'drop the entire pick' by returning coerced=None."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _coerce_target_objects_for_skill,
    )
    coerced, dropped = _coerce_target_objects_for_skill(
        skill_id="lever-4-join-discovery",
        target_kind="base_table",
        target_min_count=2,
        raw_targets=[
            "cat.sch.fact_bookings",  # valid
            "cat.sch.mv_revenue_daily",  # filtered out
        ],
        allowlist=_sample_allowlist(),
    )
    assert coerced is None, (
        "post-filter target count (1) < target_min_count (2); "
        "helper must signal drop-entire-pick by returning None"
    )
    assert "cat.sch.mv_revenue_daily" in dropped


def test_coerce_accepts_column_fqn_under_base_table_kind():
    """Column-level FQNs (catalog.schema.table.column) must validate
    against the table portion. Stage-2 L1 uses column FQNs for
    add_column_description patches; we must not drop them."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _coerce_target_objects_for_skill,
    )
    coerced, dropped = _coerce_target_objects_for_skill(
        skill_id="lever-1-table-column-description",
        target_kind="base_table",
        target_min_count=0,
        raw_targets=["cat.sch.fact_bookings.hotel_key"],
        allowlist=_sample_allowlist(),
    )
    assert coerced == ["cat.sch.fact_bookings.hotel_key"]
    assert dropped == []


def test_coerce_passes_through_when_kind_is_mixed():
    """target_kind='mixed' (lever-5a-instructions) accepts any
    identifier from any allowlist bucket."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _coerce_target_objects_for_skill,
    )
    coerced, dropped = _coerce_target_objects_for_skill(
        skill_id="lever-5a-instructions",
        target_kind="mixed",
        target_min_count=0,
        raw_targets=[
            "cat.sch.fact_bookings",
            "cat.sch.mv_revenue_daily",
            "cat.sch.tvf_revenue_by_channel",
        ],
        allowlist=_sample_allowlist(),
    )
    assert coerced == [
        "cat.sch.fact_bookings",
        "cat.sch.mv_revenue_daily",
        "cat.sch.tvf_revenue_by_channel",
    ]
    assert dropped == []


def test_coerce_empty_raw_targets_passes_through_when_min_count_zero():
    """Empty target_objects is valid when target_min_count=0 — the
    adapter treats it as 'all relevant in cluster'."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _coerce_target_objects_for_skill,
    )
    coerced, dropped = _coerce_target_objects_for_skill(
        skill_id="lever-1-table-column-description",
        target_kind="base_table",
        target_min_count=0,
        raw_targets=[],
        allowlist=_sample_allowlist(),
    )
    assert coerced == []
    assert dropped == []


def test_coerce_drops_pick_when_empty_below_min_count():
    """Empty target_objects is invalid for lever-4 (min_count=2).
    Helper must return None so caller drops the whole pick."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _coerce_target_objects_for_skill,
    )
    coerced, dropped = _coerce_target_objects_for_skill(
        skill_id="lever-4-join-discovery",
        target_kind="base_table",
        target_min_count=2,
        raw_targets=[],
        allowlist=_sample_allowlist(),
    )
    assert coerced is None
    assert dropped == []


# ── Section: Stage-1 coercion integration (Task 5) ────────────────────


def _coercion_metadata_snapshot():
    """Snapshot with two base tables + one MV — used by the integration
    tests below to exercise coercion against a realistic allowlist."""
    return {
        "config": {"description": "test"},
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.fact_orders",
                    "column_configs": [{"column_name": "order_id"}],
                    "join_specs": [],
                },
                {
                    "identifier": "cat.sch.dim_product",
                    "column_configs": [{"column_name": "product_id"}],
                    "join_specs": [],
                },
            ],
            "metric_views": [{"identifier": "cat.sch.mv_revenue_daily"}],
            "functions": [],
        },
    }


def _coercion_cluster():
    """Cluster whose blame_set includes the test's targets so the
    per-AG allowlist filter keeps them in scope."""
    return {
        "cluster_id": "C1",
        "root_cause": "missing_join_spec",
        "asi_blame_set": [
            "cat.sch.fact_orders",
            "cat.sch.dim_product",
            "cat.sch.mv_revenue_daily",
        ],
        "affected_judge": "schema_accuracy",
        "question_ids": ["Q1"],
        "signal_type": "hard",
    }


def test_stage_1_discovery_filters_mismatched_target_objects(monkeypatch):
    """An LLM response that picks lever-4-join-discovery with an MV
    FQN must have that MV filtered out. If the post-filter count is
    still >= target_min_count, keep the pick with the coerced list."""
    from genie_space_optimizer.optimization import optimizer

    def _fake_llm_openai(*args, **kwargs):
        return (
            '{"applicable_skills": ['
            '{"skill_id": "lever-4-join-discovery",'
            ' "target_objects": ['
            '   "cat.sch.fact_orders",'
            '   "cat.sch.dim_product",'
            '   "cat.sch.mv_revenue_daily"'
            ' ],'
            ' "expected_impact_qids": ["Q1"],'
            ' "evidence_refs": ["trace://q1"],'
            ' "why": "missing join", "priority": 1}'
            '], "discovery_rationale": "missing join"}',
            None,
        )
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    result = optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="missing join",
        clusters=[_coercion_cluster()],
        metadata_snapshot=_coercion_metadata_snapshot(),
        w=None,
    )
    assert len(result["applicable_skills"]) == 1
    pick = result["applicable_skills"][0]
    assert pick["skill_id"] == "lever-4-join-discovery"
    # MV must be coerced out; the two base tables remain (>= min_count=2)
    assert "cat.sch.mv_revenue_daily" not in pick["target_objects"]
    assert len(pick["target_objects"]) == 2


def test_stage_1_discovery_drops_pick_below_min_count(monkeypatch):
    """An LLM response that picks lever-4 with only one valid base
    table (after coercion) must drop the entire pick."""
    from genie_space_optimizer.optimization import optimizer

    def _fake_llm_openai(*args, **kwargs):
        return (
            '{"applicable_skills": ['
            '{"skill_id": "lever-4-join-discovery",'
            ' "target_objects": ['
            '   "cat.sch.fact_orders",'
            '   "cat.sch.mv_revenue_daily"'
            ' ],'
            ' "expected_impact_qids": ["Q1"], "evidence_refs": [],'
            ' "why": "?", "priority": 1}'
            '], "discovery_rationale": "single-table"}',
            None,
        )
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    result = optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="?",
        clusters=[_coercion_cluster()],
        metadata_snapshot=_coercion_metadata_snapshot(),
        w=None,
    )
    assert result["applicable_skills"] == [], (
        "lever-4 with single valid base table after coercion must "
        "be dropped (min_count=2 violated)"
    )


def test_stage_1_discovery_passes_through_valid_picks_unchanged(monkeypatch):
    """A well-formed Stage-1 response with valid target_objects must
    survive coercion byte-identical."""
    from genie_space_optimizer.optimization import optimizer

    def _fake_llm_openai(*args, **kwargs):
        return (
            '{"applicable_skills": ['
            '{"skill_id": "lever-1-table-column-description",'
            ' "target_objects": ["cat.sch.fact_orders"],'
            ' "expected_impact_qids": ["Q1"], "evidence_refs": [],'
            ' "why": "missing column desc", "priority": 1}'
            '], "discovery_rationale": "metadata gap"}',
            None,
        )
    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_llm_openai)

    result = optimizer._call_llm_for_stage_1_discovery(
        ag_id="AG1",
        root_cause_summary="?",
        clusters=[_coercion_cluster()],
        metadata_snapshot=_coercion_metadata_snapshot(),
        w=None,
    )
    assert len(result["applicable_skills"]) == 1
    assert result["applicable_skills"][0]["target_objects"] == [
        "cat.sch.fact_orders"
    ]


# ── Section: Stage-2 dispatcher defensive guard (Task 6) ──────────────


def test_stage_2_dispatcher_rejects_bundle_with_target_kind_mismatch(caplog):
    """Belt-and-suspenders: even if Stage-1 coercion is bypassed (e.g.
    a future code path constructs a bundle directly), the Stage-2
    dispatcher must refuse bundles whose target_objects don't match
    the skill's target_kind, log an ERROR, and return empty proposals
    without calling the underlying LLM."""
    import logging
    from genie_space_optimizer.optimization.activation_bundle import (
        ActivationBundle,
    )
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _stage_2_for_skill,
    )

    # Construct a deliberately mismatched bundle: lever-2
    # (target_kind=metric_view) handed a base-table FQN.
    bundle = ActivationBundle(
        skill_id="lever-2-mv-column-refinement",
        ag_id="AG1",
        target_objects=("cat.sch.fact_bookings",),  # WRONG — base table
        cluster_afs=(),
        metadata_snapshot={
            "tables_short": {"fact_bookings"},
            "metric_views": [],
            "functions_short": set(),
        },
        identifier_allowlist="",
        evidence_refs=(),
        expected_impact_qids=(),
        raw_evidence=(),
        lever_directives_legacy=None,
        discovery_rationale="",
        priority=1,
    )
    with caplog.at_level(logging.ERROR):
        result = _stage_2_for_skill(bundle, w=None)
    assert result["proposals"] == []
    assert "target_kind mismatch" in result.get("error", "").lower()
    assert any(
        "target_kind mismatch" in record.message.lower()
        for record in caplog.records
    )


# ── Section: Stage-2 lever-6 strategist_hints wiring (lever-6 plan Task 7) ─


def test_stage_2_l6_pipes_target_objects_to_strategist_hints(monkeypatch):
    """G10 (2026-05-17 lever-6 hardening plan, Task 7) — _stage_2_l6 must
    extract bundle.target_objects and forward as strategist_hints so
    Stage-1's identifier picks reach lever-6.

    Empirical baseline: 48/48 Trial-5 lever-6 prompts showed
    '(No strategist hints.)' — Stage-1 picks were silently discarded.
    """
    from genie_space_optimizer.optimization import three_stage_pipeline as tsp
    from genie_space_optimizer.optimization import optimizer

    captured: dict = {}

    def _fake_generate(cluster, metadata_snapshot, **kwargs):
        captured["strategist_hints"] = kwargs.get("strategist_hints")
        return None  # short-circuit; we only care about the kwarg

    monkeypatch.setattr(
        optimizer, "_generate_lever6_proposal", _fake_generate,
    )

    from types import SimpleNamespace
    bundle = SimpleNamespace(
        skill_id="lever-6-sql-expression",
        ag_id="AG1",
        cluster_afs=[{"cluster_id": "c1", "root_cause": "missing_filter"}],
        metadata_snapshot={"data_sources": {"tables": []}},
        raw_evidence=(),
        target_objects=[
            "cat.sch.tkt_payment",
            "cat.sch.tkt_payment.PAYMENT_AMT",
        ],
    )

    tsp._stage_2_l6(bundle, w=None)

    hints = captured.get("strategist_hints")
    assert hints is not None, (
        "_stage_2_l6 must pass non-None strategist_hints when bundle has target_objects"
    )
    serialized = str(hints)
    assert "tkt_payment" in serialized
    assert "PAYMENT_AMT" in serialized


def test_stage_2_l6_passes_none_strategist_hints_when_no_target_objects(monkeypatch):
    """When bundle.target_objects is empty, _stage_2_l6 must pass None
    (NOT an empty list / string) so _generate_lever6_proposal's
    placeholder fallback ('(No strategist hints.)') still renders.
    """
    from genie_space_optimizer.optimization import three_stage_pipeline as tsp
    from genie_space_optimizer.optimization import optimizer

    captured: dict = {}

    def _fake_generate(cluster, metadata_snapshot, **kwargs):
        captured["strategist_hints"] = kwargs.get("strategist_hints")
        return None

    monkeypatch.setattr(
        optimizer, "_generate_lever6_proposal", _fake_generate,
    )

    from types import SimpleNamespace
    bundle = SimpleNamespace(
        skill_id="lever-6-sql-expression",
        ag_id="AG1",
        cluster_afs=[{"cluster_id": "c1", "root_cause": "missing_filter"}],
        metadata_snapshot={"data_sources": {"tables": []}},
        raw_evidence=(),
        target_objects=[],
    )

    tsp._stage_2_l6(bundle, w=None)
    assert captured["strategist_hints"] is None, (
        f"Expected None for empty target_objects, got {captured['strategist_hints']!r}"
    )
