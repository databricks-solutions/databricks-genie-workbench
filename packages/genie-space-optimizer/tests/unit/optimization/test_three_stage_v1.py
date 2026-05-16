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
    """The prompt body must enumerate every skill_id the dispatcher
    accepts. If a new skill is added to _THREE_STAGE_SKILL_NAMES,
    this test fails until the prompt is updated."""
    cfg = _reload_config_with_env({})
    p = cfg.STAGE_1_DISCOVERY_PROMPT
    for skill_id in cfg._THREE_STAGE_SKILL_NAMES:
        assert skill_id in p, f"skill {skill_id} missing from discovery prompt"


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


# ── Section 6: _call_llm_for_stage_1_discovery ────────────────────────


def test_call_llm_for_stage_1_discovery_returns_applicable_skills_shape(monkeypatch):
    from genie_space_optimizer.optimization import optimizer

    def _fake_llm_openai(*args, **kwargs):
        return (
            '{"applicable_skills": ['
            '{"skill_id": "lever-4-join-discovery",'
            ' "target_objects": ["catalog.schema.fact_orders"],'
            ' "expected_impact_qids": ["Q1"],'
            ' "evidence_refs": ["trace://q1"],'
            ' "why": "missing join", "priority": 1}'
            '], "discovery_rationale": "missing join across fact + dim"}',
            None,
        )
    monkeypatch.setattr(optimizer, "_call_llm_openai", _fake_llm_openai)

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
    assert result["applicable_skills"][0]["skill_id"] == "lever-4-join-discovery"
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
    monkeypatch.setattr(optimizer, "_call_llm_openai", _fake_llm_openai)

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
    monkeypatch.setattr(optimizer, "_call_llm_openai", _fake_llm_openai)

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
    monkeypatch.setattr(optimizer, "_call_llm_openai", _fake_llm_openai)

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
    monkeypatch.setattr(optimizer, "_call_llm_openai", _fake_llm_openai)

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
                 "target_objects": ["t1", "t2"],
                 "expected_impact_qids": ["Q1"],
                 "evidence_refs": [], "why": "join1", "priority": 1},
                {"skill_id": "lever-4-join-discovery",
                 "target_objects": ["t2", "t3"],
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
