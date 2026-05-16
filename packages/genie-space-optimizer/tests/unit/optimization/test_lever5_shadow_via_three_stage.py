"""Track B+ — Plan 2 shadow emission from the three-stage pipeline.

Track B moved the shadow-comparison emission into
``_dispatch_lever_5_split``. That covers AGs the legacy strategist
routes through the L5 selector → dispatcher path, but it does NOT
cover Plan 3's three-stage pipeline, whose stage-2 adapters
(``_stage_2_l5a`` / ``_stage_2_l5b``) call the L5 skill functions
directly and bypass the dispatcher entirely.

Trial-3 ran with Plan 3 shadow on (``GSO_THREE_STAGE_SHADOW_V1=1``),
the L5 skills fired twice each (hits counter), but
``_emit_lever5_shadow_comparison`` was never called → 0 records →
Plan 2 fixture coverage stayed empty.

This file pins the contract: when the three-stage pipeline picks an
L5 skill (5a or 5b) and ``GSO_LEVER5_SHADOW_V1=1``, the pipeline must
emit exactly one shadow-comparison record per AG. When no L5 skill
is picked, or when shadow is off, the pipeline must NOT call the
holistic — that would double the L5 LLM cost in production.
"""
from __future__ import annotations

import importlib
import os


_PLAN2_ENV_KEYS = (
    "GSO_LEVER5_SPLIT_V1",
    "GSO_LEVER5_SHADOW_V1",
    "GSO_LEVER5_SPLIT_CAPTURE_PATH",
    "GSO_LEVER5_SPLIT_CAPTURE_REQUIRE_COVERAGE",
)
_PLAN3_ENV_KEYS = (
    "GSO_THREE_STAGE_V1",
    "GSO_THREE_STAGE_SHADOW_V1",
    "GSO_THREE_STAGE_CAPTURE_PATH",
    "GSO_THREE_STAGE_CAPTURE_REQUIRE_COVERAGE",
)


def _reload_config_with_env(env: dict[str, str]):
    """Reload common.config with patched env covering both Plan 2 and
    Plan 3 keys (the pipeline emission contract couples the two)."""
    from genie_space_optimizer.common import config as cfg

    for key in _PLAN2_ENV_KEYS + _PLAN3_ENV_KEYS:
        if key in env:
            os.environ[key] = env[key]
        else:
            os.environ.pop(key, None)
    return importlib.reload(cfg)


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
        "root_cause": "missing_instruction",
        "asi_failure_type": "missing_instruction",
        "asi_blame_set": ["catalog.schema.fact_bookings"],
        "question_ids": ["Q1", "Q2"],
        "question_traces": [
            {"question_id": "Q1", "trace_id": "trace://q1"},
            {"question_id": "Q2", "trace_id": "trace://q2"},
        ],
    }


def _stage_1_picks_l5() -> dict:
    return {
        "applicable_skills": [
            {
                "skill_id": "lever-5a-instructions",
                "target_objects": ["catalog.schema.fact_bookings"],
                "expected_impact_qids": ["Q1", "Q2"],
                "evidence_refs": ["trace://q1"],
                "why": "missing routing instruction",
                "priority": 1,
            },
            {
                "skill_id": "lever-5b-example-sql",
                "target_objects": ["catalog.schema.fact_bookings"],
                "expected_impact_qids": ["Q1"],
                "evidence_refs": ["trace://q1"],
                "why": "missing example sql",
                "priority": 1,
            },
        ],
        "discovery_rationale": "L5 a+b coverage for AG1",
    }


def _stage_1_picks_l4_only() -> dict:
    return {
        "applicable_skills": [{
            "skill_id": "lever-4-join-discovery",
            "target_objects": ["catalog.schema.fact_bookings",
                                "catalog.schema.dim_hotel"],
            "expected_impact_qids": ["Q1"],
            "evidence_refs": ["trace://q1"],
            "why": "missing join",
            "priority": 1,
        }],
        "discovery_rationale": "L4 only — no L5 picks",
    }


def test_pipeline_emits_one_shadow_record_when_l5_picked(monkeypatch):
    """Track B+ core contract — when the three-stage pipeline picks an
    L5 skill (5a or 5b) and ``GSO_LEVER5_SHADOW_V1=1``, the pipeline
    must emit exactly one shadow-comparison record per AG. Without
    this, Plan 3 (whose adapters bypass ``_dispatch_lever_5_split``)
    would never populate Plan 2 fixture coverage."""
    cfg = _reload_config_with_env({"GSO_LEVER5_SHADOW_V1": "1"})
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    monkeypatch.setattr(
        optimizer, "_call_llm_for_stage_1_discovery",
        lambda **kw: _stage_1_picks_l5(),
    )
    monkeypatch.setattr(
        optimizer, "_call_llm_for_lever_5a_instructions",
        lambda *a, **kw: {"instruction_text": "NEW", "rationale": "r"},
    )
    monkeypatch.setattr(
        optimizer, "_dispatch_lever_5b_for_cluster",
        lambda *a, **kw: [{"example_sql": "SELECT 1", "rationale": "r"}],
    )
    holistic_calls = {"n": 0}
    def _record(*a, **kw):
        holistic_calls["n"] += 1
        return {"instruction_text": "OLD",
                "example_sql_proposals": [],
                "rationale": "old"}
    monkeypatch.setattr(
        optimizer, "_call_llm_for_holistic_instructions", _record,
    )

    three_stage_pipeline.run_three_stage_pipeline_for_ag(
        ag_id="AG1",
        root_cause_summary="missing routing",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )

    snap = cfg.dump_lever5_split_capture_summary()
    assert snap["shadow_comparisons"] == 1, (
        f"three-stage pipeline must emit exactly one shadow "
        f"comparison when an L5 skill is picked; got "
        f"{snap['shadow_comparisons']!r}; full snapshot={snap!r}"
    )
    assert holistic_calls["n"] == 1, (
        f"holistic must run once to compute the shadow comparison; "
        f"got {holistic_calls['n']!r}"
    )


def test_pipeline_skips_emission_when_no_l5_picked(monkeypatch):
    """Defensive: when Stage-1 picks only non-L5 skills (e.g. L4 only),
    the pipeline must NOT call the holistic and must NOT emit a
    shadow-comparison record. Otherwise every AG would pay the
    holistic LLM cost regardless of whether L5 is in scope."""
    cfg = _reload_config_with_env({"GSO_LEVER5_SHADOW_V1": "1"})
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    monkeypatch.setattr(
        optimizer, "_call_llm_for_stage_1_discovery",
        lambda **kw: _stage_1_picks_l4_only(),
    )
    monkeypatch.setattr(
        optimizer, "_call_llm_for_join_discovery",
        lambda metadata_snapshot, hints, w=None, **kw: [{
            "join_spec": {"left_table": "t1", "right_table": "t2"},
            "rationale": "r",
        }],
    )
    holistic_calls = {"n": 0}
    def _record(*a, **kw):
        holistic_calls["n"] += 1
        return {"instruction_text": "OLD",
                "example_sql_proposals": [],
                "rationale": "old"}
    monkeypatch.setattr(
        optimizer, "_call_llm_for_holistic_instructions", _record,
    )

    three_stage_pipeline.run_three_stage_pipeline_for_ag(
        ag_id="AG1",
        root_cause_summary="missing join",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )

    snap = cfg.dump_lever5_split_capture_summary()
    assert snap["shadow_comparisons"] == 0, (
        f"pipeline must not emit when no L5 skill is picked; got "
        f"{snap['shadow_comparisons']!r}; full snapshot={snap!r}"
    )
    assert holistic_calls["n"] == 0, (
        f"holistic must not run when no L5 skill is picked — that "
        f"would double L5 LLM cost in production; got "
        f"{holistic_calls['n']!r}"
    )


def test_pipeline_skips_emission_when_shadow_flag_off(monkeypatch):
    """Defensive: with GSO_LEVER5_SHADOW_V1 off, the pipeline must NOT
    call the holistic and must NOT emit, even when L5 skills are
    picked. Protects production cost when shadow capture is
    disabled."""
    cfg = _reload_config_with_env({"GSO_LEVER5_SHADOW_V1": "0"})
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization import three_stage_pipeline

    monkeypatch.setattr(
        optimizer, "_call_llm_for_stage_1_discovery",
        lambda **kw: _stage_1_picks_l5(),
    )
    monkeypatch.setattr(
        optimizer, "_call_llm_for_lever_5a_instructions",
        lambda *a, **kw: {"instruction_text": "NEW", "rationale": "r"},
    )
    monkeypatch.setattr(
        optimizer, "_dispatch_lever_5b_for_cluster",
        lambda *a, **kw: [{"example_sql": "SELECT 1", "rationale": "r"}],
    )
    holistic_calls = {"n": 0}
    def _record(*a, **kw):
        holistic_calls["n"] += 1
        return {"instruction_text": "OLD",
                "example_sql_proposals": [],
                "rationale": "old"}
    monkeypatch.setattr(
        optimizer, "_call_llm_for_holistic_instructions", _record,
    )

    three_stage_pipeline.run_three_stage_pipeline_for_ag(
        ag_id="AG1",
        root_cause_summary="missing routing",
        clusters=[_sample_cluster()],
        metadata_snapshot=_sample_metadata_snapshot(),
        w=None,
    )

    snap = cfg.dump_lever5_split_capture_summary()
    assert snap["shadow_comparisons"] == 0, (
        f"pipeline must not emit when shadow flag is off; got "
        f"{snap['shadow_comparisons']!r}"
    )
    assert holistic_calls["n"] == 0, (
        f"holistic must not run when shadow flag is off — that "
        f"would double L5 LLM cost in production; got "
        f"{holistic_calls['n']!r}"
    )
